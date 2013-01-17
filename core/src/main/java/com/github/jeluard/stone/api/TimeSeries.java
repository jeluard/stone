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
import com.github.jeluard.guayaba.base.Preconditions2;
import com.github.jeluard.stone.spi.Consolidator;
import com.github.jeluard.stone.spi.Dispatcher;
import com.github.jeluard.stone.spi.Storage;
import com.github.jeluard.stone.spi.StorageFactory;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.joda.time.DateTime;

public class TimeSeries implements Closeable {

  private static final Logger LOGGER = Logger.getLogger("com.github.jeluard.stone");

  private final String id;
  private final Collection<Archive> archives;
  private final Dispatcher dispatcher;
  private final Map<Pair<Archive, SamplingWindow>, Storage> storages;
  private final AtomicReference<Long> beginning;
  private final AtomicReference<Long> latest;

  public TimeSeries(final String id, final Collection<Archive> archives, final Dispatcher dispatcher, final StorageFactory storageFactory) throws IOException {
    this.id = Preconditions.checkNotNull(id, "null id");//TODO id must be unique per-vm
    this.archives = new ArrayList<Archive>(Preconditions2.checkNotEmpty(archives, "null archives"));
    this.dispatcher = Preconditions.checkNotNull(dispatcher, "null dispatcher");
    this.storages = new HashMap<Pair<Archive, SamplingWindow>, Storage>(createStorages(storageFactory, id, archives));
    this.beginning = new AtomicReference<Long>(extractBeginning(this.storages.values().iterator().next()));
    this.latest = new AtomicReference<Long>(extractLatest(this.storages.values()));
  }

  private Map<Pair<Archive, SamplingWindow>, Storage> createStorages(final StorageFactory storageFactory, final String id, final Collection<Archive> archives) throws IOException {
    final Map<Pair<Archive, SamplingWindow>, Storage> newStorages = new HashMap<Pair<Archive, SamplingWindow>, Storage>();
    for (final Archive archive : archives) {
      for (final SamplingWindow samplingWindow : archive.getSamplingWindows()) {
        newStorages.put(new Pair<Archive, SamplingWindow>(archive, samplingWindow), storageFactory.createOrOpen(id, archive, samplingWindow));
      }
    }
    return newStorages;
  }

  /**
   * @param storage
   * @return beginning of {@link Storage#beginning()} if any; null otherwise
   * @throws IOException 
   */
  private Long extractBeginning(final Storage storage) throws IOException {
    final Optional<DateTime> interval = storage.beginning();
    if (interval.isPresent()) {
      return interval.get().getMillis();
    }
    return null;
  }

  /**
   * @param storages
   * @return latest (more recent) timestamp stored in specified {@link archives}; null if all archive are empty
   * @throws IOException 
   */
  private Long extractLatest(final Collection<Storage> storages) throws IOException {
    Long storageLatest = null;
    for (final Storage storage : storages) {
      final Optional<DateTime> optionalInterval = storage.end(); 
      if (optionalInterval.isPresent()) {
        final long endInterval = optionalInterval.get().getMillis();
        if (storageLatest == null) {
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

  public Map<Pair<Archive, SamplingWindow>, Storage> getStorages() {
    return Collections.unmodifiableMap(this.storages);
  }

  private Storage getStorage(final Archive archive, final SamplingWindow samplingWindow) throws IOException {
    return this.storages.get(new Pair<Archive, SamplingWindow>(archive, samplingWindow));
  }

  private long windowId(final long beginning, final long timestamp, final long duration) {
    return (timestamp - beginning) / duration;
  }

  private void accumulate(final long timestamp, final int value, final Collection<Consolidator> consolidators) {
    this.dispatcher.accumulate(timestamp, value, consolidators);
  }

  private void persist(final long timestamp, final Storage storage, final Collection<Consolidator> consolidators) throws IOException {
    storage.append(timestamp, this.dispatcher.reduce(consolidators));
  }

  private void checkNotBeforeLatestTimestamp(final Long previousTimestamp, final long currentTimestamp) {
    if (previousTimestamp != null && !(currentTimestamp > previousTimestamp)) {
      throw new IllegalArgumentException("Provided timestamp from <"+currentTimestamp+"> must be more recent than <"+previousTimestamp+">");
    }
  }

  private Long recordLatest(final long timestamp) {
    //Atomically set to the new timestamp if value is null (i.e. no value as yet been recorded)
    final Long previousTimestamp = this.latest.getAndSet(timestamp);
    checkNotBeforeLatestTimestamp(previousTimestamp, timestamp);
    return previousTimestamp;
  }

  private synchronized long inferBeginning(final long timestamp) {
    //If beginning is still null (i.e. all storages where empty) sets it's value to timestamp
    //This will be done only once (hence track the first timestamp received)
    this.beginning.compareAndSet(null, timestamp);
    return this.beginning.get();
  }

  //https://blogs.oracle.com/dholmes/entry/inside_the_hotspot_vm_clocks
  public void publish(final long timestamp, final int value) throws IOException {
    Preconditions.checkNotNull(timestamp, "null timestamp");

    //TODO check thread-safety
    final Long previousTimestamp = recordLatest(timestamp);
    final long beginningTimestamp = inferBeginning(timestamp);

    //TODO // ?
    for (final Archive archive : this.archives) {
      //TODO sort by window resolution, ts with same frame can be optimized
      final Collection<Consolidator> consolidators = archive.getConsolidators();
      for (final SamplingWindow samplingWindow : archive.getSamplingWindows()) {
        accumulate(timestamp, value, consolidators);

        if (previousTimestamp != null) {
          final long duration = samplingWindow.getResolution().getMillis();
          final long currentWindowId = windowId(beginningTimestamp, timestamp, duration);
          final long previousWindowId = windowId(beginningTimestamp, previousTimestamp, duration);
          if (currentWindowId != previousWindowId) {
            //previousTimestamp will be null on first run with empty archives
            final long previousWindowBeginning = beginningTimestamp + previousWindowId * duration;
            persist(previousWindowBeginning, getStorage(archive, samplingWindow), consolidators);
          }
        }
      }
    }
  }

  /**
   * Calls {@link Closeable#close()} on all {@link Storage} implementing {@link Closeable}.
   *
   * @throws IOException 
   */
  @Override
  public void close() throws IOException {
    for (final Storage storage : this.storages.values()) {
      try {
        if (storage instanceof Closeable) {
          Closeable.class.cast(storage).close();
        }
      } catch (IOException e) {
        if (TimeSeries.LOGGER.isLoggable(Level.WARNING)) {
          TimeSeries.LOGGER.log(Level.WARNING, "Got an exception while closing <"+storage+">", e);
        }
      }
    }
  }

}