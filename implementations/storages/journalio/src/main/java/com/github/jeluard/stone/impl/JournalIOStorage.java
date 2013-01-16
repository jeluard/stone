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
package com.github.jeluard.stone.impl;

import com.github.jeluard.guayaba.base.Pair;
import com.github.jeluard.stone.spi.BaseBinaryStorage;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.AbstractIterator;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;

import journal.io.api.Journal;
import journal.io.api.Location;
import journal.io.api.WriteCallback;

import org.joda.time.DateTime;

/**
 * Reads are down in {@link Journal.ReadType#ASYNC} mode as no delete is performed.
 */
public class JournalIOStorage extends BaseBinaryStorage implements Closeable {

  private static final WriteCallback LOGGING_WRITE_CALLBACK = new WriteCallback() {
    @Override
    public void onSync(final Location syncedLocation) {
    }
    @Override
    public void onError(final Location location, final Throwable error) {
    }
  };

  private final Journal journal;

  /**
   * 
   * @param journal life-cycle won't be handled here, manually initialize/clean
   * @param compressor 
   */
  public JournalIOStorage(final Journal journal) throws IOException {
    this.journal = Preconditions.checkNotNull(journal, "null journal");
  }

  @Override
  protected final void append(final ByteBuffer buffer) throws IOException {
    this.journal.write(buffer.array(), Journal.WriteType.SYNC, JournalIOStorage.LOGGING_WRITE_CALLBACK);
  }

  protected final byte[] readNextLocation(final Iterator<Location> locations) throws IOException {
    return this.journal.read(locations.next(), Journal.ReadType.SYNC);
  }

  protected final Optional<DateTime> nextTimestampIfAny(final Iterator<Location> locations) throws IOException {
    if (!locations.hasNext()) {
      return Optional.absent();
    }

    return Optional.of(new DateTime(getTimestamp(readNextLocation(locations))));
  }

  @Override
  public final Optional<DateTime> beginning() throws IOException {
    return nextTimestampIfAny(this.journal.redo().iterator());
  }

  @Override
  public final Optional<DateTime> end() throws IOException {
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
                final long timestamp = getTimestamp(readNextLocation(locations));
                final int[] consolidates = getConsolidates(readNextLocation(locations));
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

  @Override
  public void close() throws IOException {
    this.journal.close();
  }

}