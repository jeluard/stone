/**
 * Copyright 2012 Julien Eluard
 * This project includes software developed by Julien Eluard: https://github.com/jeluard/
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.jeluard.stone.storage.journalio;

import com.github.jeluard.guayaba.base.Pair;
import com.github.jeluard.stone.spi.ByteBufferStorage;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Iterables;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;

import journal.io.api.Journal;
import journal.io.api.Location;
import journal.io.api.WriteCallback;

/**
 * A {@link Storage} implementation relying on <a href="https://github.com/sbtourist/Journal.IO">Journal.IO</a>.
 * <br>
 * Data is stored in the format:
 *
 * +-----------+---------------+-----+---------------+-----------+-----+
 * | timestamp | consolidate-1 | ... | consolidate-n | timestamp | ... |
 * +-----------+---------------+-----+---------------+-----------+-----+
 *
 * Timestamp are not necessarily consecutive. Missing window are not encoded.
 * <br>
 * Estimated size on disk is 8 (timestamp) + 4 * n (n being number of consolidates) + 30 (Journal.IO overhead per location).
 * <br>
 * Journal.IO has an overhead of 21 bytes per written batch plus 9 bytes per element contained in the batch.
 * <br>
 * <br>
 * This format is both easy to implement and fast to parse but waste significant space.
 */
public final class JournalIOStorage extends ByteBufferStorage implements Closeable {

  public static final WriteCallback DEFAULT_WRITE_CALLBACK = new WriteCallback() {
    @Override
    public void onSync(final Location location) {
      if (JournalIOStorageFactory.LOGGER.isLoggable(Level.FINEST)) {
        JournalIOStorageFactory.LOGGER.log(Level.FINEST, "Succesfully writen at location <{0}>", location);
      }
    }
    @Override
    public void onError(final Location location, final Throwable e) {
      if (JournalIOStorageFactory.LOGGER.isLoggable(Level.WARNING)) {
        JournalIOStorageFactory.LOGGER.log(Level.WARNING, "Failed to write at location <"+location+">", e);
      }
    }
  };

  private final Journal journal;
  private final WriteCallback writeCallback;
  private final AtomicInteger writes;
  private volatile boolean isFull = false;

  /**
   * @param maximumSize 
   * @param journal life-cycle is not handled here, expect a fully {@link Journal#open()} {@code journal}
   * @param writeCallback
   */
  public JournalIOStorage(final int maximumSize, final Journal journal, final WriteCallback writeCallback) throws IOException {
    super(maximumSize);

    this.journal = Preconditions.checkNotNull(journal, "null journal");
    this.writeCallback = Preconditions.checkNotNull(writeCallback, "null writeCallback");
    this.writes = new AtomicInteger(Iterables.size(all()));
  }

  private boolean isFull() {
    if (this.isFull) {
      return true;
    }

    final int size = this.writes.incrementAndGet();
    if (size > getMaximumSize()) {
      this.isFull = true;
      return true;
    }
    return false;
  }

  /**
   * Remove first value currently stored.
   *
   * @throws IOException 
   */
  private void removeFirst() throws IOException {
    this.journal.delete(this.journal.redo().iterator().next());
  }

  @Override
  protected void append(final ByteBuffer buffer) throws IOException {
    this.journal.write(buffer.array(), Journal.WriteType.SYNC, this.writeCallback);

    //If maximuSize elements are stored remove the first one.
    if (isFull()) {
      removeFirst();
    }
  }

  /**
   * @param locations
   * @return the content as {@link byte[]} of the next {@link Location} from {@code locations}
   * @throws IOException 
   */
  private byte[] readNextLocation(final Iterator<Location> locations) throws IOException {
    return this.journal.read(locations.next(), Journal.ReadType.SYNC);
  }

  /**
   * @param locations
   * @return the next timestamp among {@link locations} if any
   * @throws IOException 
   */
  private Optional<Long> nextTimestampIfAny(final Iterator<Location> locations) throws IOException {
    if (!locations.hasNext()) {
      return Optional.absent();
    }

    return Optional.of(getTimestamp(readNextLocation(locations)));
  }

  @Override
  public Optional<Long> beginning() throws IOException {
    return nextTimestampIfAny(this.journal.redo().iterator());
  }

  @Override
  public Optional<Long> end() throws IOException {
    return nextTimestampIfAny(this.journal.undo().iterator());
  }

  @Override
  public Iterable<Pair<Long, int[]>> all() throws IOException {
    final Iterator<Location> locations = JournalIOStorage.this.journal.redo().iterator();
    return new Iterable<Pair<Long, int[]>>() {
      @Override
      public Iterator<Pair<Long, int[]>> iterator() {
        return new AbstractIterator<Pair<Long, int[]>>() {
          @Override
          protected Pair<Long, int[]> computeNext() {
            if (locations.hasNext()) {
              try {
                //Read next location. It will contain the timestamp then all consolidates.
                final byte[] nextLocation = readNextLocation(locations);
                final long timestamp = getTimestamp(nextLocation);
                final int[] consolidates = getConsolidates(nextLocation, bits2Bytes(Long.SIZE));
                return new Pair<Long, int[]>(timestamp, consolidates);
              } catch (IOException e) {
                throw new RuntimeException(e);
              }
            }
            return endOfData();
          }
        };
      }
    };
  }

  /**
   * Triggers {@link Journal#compact()}.
   * Note that compaction does not apply to file currently written to but older ones.
   *
   * @throws IOException 
   */
  void compact() throws IOException {
    this.journal.compact();
  }

  @Override
  public void close() throws IOException {
    this.journal.close();
  }

}