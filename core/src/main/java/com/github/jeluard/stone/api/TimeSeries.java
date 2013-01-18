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
package com.github.jeluard.stone.api;

import com.github.jeluard.guayaba.base.Pair;
import com.github.jeluard.stone.impl.Engine;
import com.github.jeluard.stone.spi.Dispatcher;
import com.github.jeluard.stone.spi.Storage;
import com.github.jeluard.stone.spi.StorageFactory;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.AtomicReference;

import org.joda.time.DateTime;

public final class TimeSeries implements Closeable {

  private static final Set<String> IDS = new CopyOnWriteArraySet<String>();

  private final String id;
  private final Engine engine;
  private long beginning;
  private long latest;

  public TimeSeries(final String id, final Collection<Archive> archives, final Dispatcher dispatcher, final StorageFactory storageFactory) throws IOException {
    this.id = Preconditions.checkNotNull(id, "null id");
    if (!TimeSeries.IDS.add(id)) {
      throw new IllegalArgumentException("ID <"+id+"> is already used");
    }
    this.engine = new Engine(id, archives, dispatcher, storageFactory);
    this.beginning = extractBeginning(getStorages().values().iterator().next());
    this.latest = extractLatest(getStorages().values());
  }

  /**
   * @param storage
   * @return beginning of {@link Storage#beginning()} if any; null otherwise
   * @throws IOException 
   */
  private long extractBeginning(final Storage storage) throws IOException {
    final Optional<DateTime> interval = storage.beginning();
    if (interval.isPresent()) {
      return interval.get().getMillis();
    }
    return 0L;
  }

  /**
   * @param storages
   * @return latest (more recent) timestamp stored in specified {@link archives}; null if all archive are empty
   * @throws IOException 
   */
  private long extractLatest(final Collection<Storage> storages) throws IOException {
    long storageLatest = 0L;
    for (final Storage storage : storages) {
      final Optional<DateTime> optionalInterval = storage.end(); 
      if (optionalInterval.isPresent()) {
        final long endInterval = optionalInterval.get().getMillis();
        if (storageLatest == 0L) {
          storageLatest = endInterval;
        } else if (endInterval > storageLatest) {
          storageLatest = endInterval;
        }
      }
    }
    return storageLatest;
  }

  public String getId() {
    return this.id;
  }

  public Map<Pair<Archive, Window>, Storage> getStorages() {
    return this.engine.getStorages();
  }

  private void checkNotBeforeLatestTimestamp(final long previousTimestamp, final long currentTimestamp) {
    if (!(currentTimestamp > previousTimestamp)) {
      throw new IllegalArgumentException("Provided timestamp from <"+currentTimestamp+"> must be more recent than <"+previousTimestamp+">");
    }
  }

  private long recordLatest(final long timestamp) {
    //Atomically set to the new timestamp if value is null (i.e. no value as yet been recorded)
    final long previousTimestamp = this.latest;//.getAndSet(timestamp);
    latest = timestamp;
    checkNotBeforeLatestTimestamp(previousTimestamp, timestamp);
    return previousTimestamp;
  }

  private synchronized long inferBeginning(final long timestamp) {
    //If beginning is still null (i.e. all storages where empty) sets it's value to timestamp
    //This will be done only once (hence track the first timestamp received)
    if (this.beginning == 0L) {
      this.beginning = timestamp;
    }
    //this.beginning.compareAndSet(0L, timestamp);
    return this.beginning;//.get();
  }

  //https://blogs.oracle.com/dholmes/entry/inside_the_hotspot_vm_clocks
  public void publish(final long timestamp, final int value) throws IOException {
    Preconditions.checkNotNull(timestamp, "null timestamp");

    //TODO check thread-safety
    final long previousTimestamp = recordLatest(timestamp);
    final long beginningTimestamp = inferBeginning(timestamp);

    //TODO // ?
    this.engine.publish(beginningTimestamp, previousTimestamp, timestamp, value);
  }

  /**
   * Calls {@link Closeable#close()} on all {@link Storage} implementing {@link Closeable}.
   *
   * @throws IOException 
   */
  @Override
  public void close() throws IOException {
    TimeSeries.IDS.remove(this.id);

    this.engine.close();
  }

}