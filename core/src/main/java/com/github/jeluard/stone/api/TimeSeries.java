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

import org.joda.time.Interval;

public class TimeSeries implements Closeable {

  private static final class StorageKey {

    private final String id;
    private final Archive archive;
    private final SamplingWindow samplingWindow;

    public StorageKey(final String id, final Archive archive, final SamplingWindow samplingWindow) {
      this.id = Preconditions.checkNotNull(id, "null id");
      this.archive = Preconditions.checkNotNull(archive, "null archive");
      this.samplingWindow = Preconditions.checkNotNull(samplingWindow, "null samplingWindow");
    }

    @Override
    public int hashCode() {
      return this.id.hashCode()+this.archive.hashCode()+this.samplingWindow.hashCode();
    }

    @Override
    public boolean equals(final Object object) {
      if (!(object instanceof StorageKey)) {
        return false;
      }

      final StorageKey other = (StorageKey) object;
      return this.id.equals(other.id) && this.archive.equals(other.archive) && this.samplingWindow.equals(other.samplingWindow);
    }

  }

  private static final Logger LOGGER = Logger.getLogger("com.github.jeluard.stone");

  private final String id;
  private final Collection<Archive> archives;
  private final Dispatcher dispatcher;
  private final Map<StorageKey, Storage> storages;
  private final AtomicReference<Long> beginning;
  private final AtomicReference<Long> latest;

  public TimeSeries(final String id, final Collection<Archive> archives, final Dispatcher dispatcher, final StorageFactory storageFactory) throws IOException {
    this.id = Preconditions.checkNotNull(id, "null id");
    this.archives = new ArrayList<Archive>(Preconditions2.checkNotEmpty(archives, "null archives"));
    this.dispatcher = Preconditions.checkNotNull(dispatcher, "null dispatcher");
    this.storages = new HashMap<StorageKey, Storage>(createStorages(storageFactory, id, archives));
    this.beginning = new AtomicReference<Long>(extractBeginning(this.storages.values().iterator().next()));
    this.latest = new AtomicReference<Long>(extractLatest(this.storages.values()));
  }

  private Map<StorageKey, Storage> createStorages(final StorageFactory storageFactory, final String id, final Collection<Archive> archives) throws IOException {
    final Map<StorageKey, Storage> newStorages = new HashMap<StorageKey, Storage>();
    for (final Archive archive : archives) {
      for (final SamplingWindow samplingWindow : archive.getSamplingWindows()) {
        newStorages.put(new StorageKey(id, archive, samplingWindow), storageFactory.createOrOpen(id, archive, samplingWindow));
      }
    }
    return newStorages;
  }

  /**
   * @param storage
   * @return beginning of {@link Storage#interval()} if any; null otherwise
   * @throws IOException 
   */
  private Long extractBeginning(final Storage storage) throws IOException {
    final Optional<Interval> interval = storage.interval();
    if (interval.isPresent()) {
      return interval.get().getStartMillis();
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
      final Optional<Interval> optionalInterval = storage.interval(); 
      if (optionalInterval.isPresent()) {
        final long endInterval = optionalInterval.get().getEndMillis();
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

  public Collection<Archive> getArchives() {
    return Collections.unmodifiableCollection(this.archives);
  }

  private Storage getStorage(final Archive archive, final SamplingWindow samplingWindow) throws IOException {
    return this.storages.get(new StorageKey(this.id, archive, samplingWindow));
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
    Preconditions.checkNotNull(value, "null value");

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
          final long duration = samplingWindow.getDuration().getMillis();
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