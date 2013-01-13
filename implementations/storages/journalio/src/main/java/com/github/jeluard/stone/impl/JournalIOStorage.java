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

import com.github.jeluard.stone.api.DataAggregates;
import com.github.jeluard.stone.spi.BaseStorage;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.AbstractIterator;
import com.google.common.primitives.Longs;

import java.io.IOException;
import java.util.Iterator;

import journal.io.api.Journal;
import journal.io.api.Location;
import journal.io.api.WriteCallback;

import org.joda.time.DateTime;
import org.joda.time.Interval;
/**
 * http://www.oracle.com/technetwork/database/berkeleydb/downloads/maven-087630.html
 * http://docs.oracle.com/cd/E17277_02/html/index.html
 * http://download.oracle.com/otndocs/products/berkeleydb/html/je/je-5.0.58_changelog.html
 */


/**
 * Reads are down in {@link Journal.ReadType#ASYNC} mode as no delete is performed.
 */
public class JournalIOStorage extends BaseStorage {

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
   * @param journal life-cycle won't be handled her, manually initialize/clean
   * @param compressor 
   */
  public JournalIOStorage(final Journal journal) throws IOException {
    this.journal = Preconditions.checkNotNull(journal, "null journal");
  }

  @Override
  public void append(final int[] aggregates) throws IOException {
    final byte[] bytes = Longs.toByteArray(aggregates[0]);
    //http://docs.oracle.com/javase/tutorial/java/nutsandbolts/datatypes.html
    this.journal.write(bytes, Journal.WriteType.SYNC, JournalIOStorage.LOGGING_WRITE_CALLBACK);
  }

  @Override
  public Optional<Interval> interval() throws IOException {
    final Iterator<Location> undo = this.journal.undo().iterator();
    if (!undo.hasNext()) {
      return Optional.absent();
    }
    //TODO
    return Optional.of(new Interval(DateTime.now(), DateTime.now()));//Optional.of(this.journal.read(iterator.next(), Journal.ReadType.ASYNC));
  }

  @Override
  public Iterable<DataAggregates> all() throws IOException {
    final Iterator<Location> locations = JournalIOStorage.this.journal.redo().iterator();
    return new Iterable<DataAggregates>() {
      @Override
      public Iterator<DataAggregates> iterator() {
        return new AbstractIterator<DataAggregates>() {
          @Override
          protected DataAggregates computeNext() {
            if (locations.hasNext()) {
              return null;//JournalIOStorage.this.journal.read(locations.next(), Journal.ReadType.SYNC);
            }
            return endOfData();
          }
        };
      }
    };
  }

}