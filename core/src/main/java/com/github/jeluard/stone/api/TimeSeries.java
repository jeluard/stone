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
import com.github.jeluard.guayaba.util.concurrent.ConcurrentMaps;
import com.github.jeluard.stone.spi.Consolidator;
import com.github.jeluard.stone.spi.Dispatcher;
import com.github.jeluard.stone.spi.Storage;
import com.github.jeluard.stone.spi.StorageFactory;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Suppliers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;

import org.joda.time.Interval;

public class TimeSeries {

  private final String id;
  private final Collection<Archive> archives;
  private final Dispatcher dispatcher;
  private final StorageFactory storageFactory;
  private final ConcurrentMap<Archive, Long> beginnings = new ConcurrentHashMap<Archive, Long>();
  private final AtomicReference<Long> latest;
  private final AtomicReference<Long> firstReceived = new AtomicReference<Long>(null);

  public TimeSeries(final String id, final Collection<Archive> archives, final Dispatcher dispatcher, final StorageFactory storageFactory) throws IOException {
    this.id = Preconditions.checkNotNull(id, "null id");
    this.archives = new ArrayList<Archive>(Preconditions2.checkNotEmpty(archives, "null archives"));
    this.dispatcher = Preconditions.checkNotNull(dispatcher, "null dispatcher");
    this.storageFactory = Preconditions.checkNotNull(storageFactory, "null storageFactory");
    //TODO Should all archive share beginning?? Would simplify quite a lot
    this.beginnings.putAll(extractBeginnings(archives));
    this.latest = new AtomicReference<Long>(extractLatest(archives));
  }

  private Map<Archive, Long> extractBeginnings(final Collection<Archive> archives) throws IOException {
    final Map<Archive, Long> storageBeginnings = new HashMap<Archive, Long>();
    for (final Archive archive : archives) {
      final Optional<Interval> interval = getStorage(archive).interval(); 
      if (interval.isPresent()) {
        storageBeginnings.put(archive, interval.get().getStartMillis());
      }
    }
    return storageBeginnings;
  }

  /**
   * @param archives
   * @return latest (more recent) timestamp stored in specified {@link archives}; null if all archive are empty
   * @throws IOException 
   */
  private Long extractLatest(final Collection<Archive> archives) throws IOException {
    Long storageLatest = null;
    for (final Archive archive : archives) {
      final Optional<Interval> optionalInterval = getStorage(archive).interval(); 
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

  private Storage getStorage(final Archive archive) throws IOException {
    //TODO add caching
    return this.storageFactory.createOrOpen(getId(), archive);
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

  /**
   * @param archive
   * @param firstTimestampReceived
   * @return beginning as reported by {@link Storage#interval()} or {@code firstTimestampReceived} value
   */
  private long beginningFor(final Archive archive, final long firstTimestampReceived) {
    return ConcurrentMaps.putIfAbsentAndReturn(this.beginnings, archive, Suppliers.ofInstance(firstTimestampReceived));
  }

  private long checkNotBeforeLatestTimestamp(final Long previousTimestamp, final long currentTimestamp) {
    if (previousTimestamp != null && !(currentTimestamp > previousTimestamp)) {
      throw new IllegalArgumentException("Provided timestamp from <"+currentTimestamp+"> must be more recent than <"+previousTimestamp+">");
    }
    return previousTimestamp;
  }

  private long recordLatest(final long timestamp) {
    //Atomically set to the new timestamp if value is null (i.e. no value as yet been recorded)
    final Long previousTimestamp = this.latest.getAndSet(timestamp);
    checkNotBeforeLatestTimestamp(previousTimestamp, timestamp);
    return previousTimestamp;
  }

  private synchronized long recordFirst(final long timestamp) {
    this.firstReceived.compareAndSet(null, timestamp);
    return this.firstReceived.get();
  }

  //https://blogs.oracle.com/dholmes/entry/inside_the_hotspot_vm_clocks
  public void publish(final long timestamp, final int value) throws IOException {
    Preconditions.checkNotNull(timestamp, "null timestamp");
    Preconditions.checkNotNull(value, "null value");

    //TODO check thread-safety
    final Long previousTimestamp = recordLatest(timestamp);
    final long firstTimestamp = recordFirst(timestamp);

    //TODO // ?
    for (final Archive archive : this.archives) {
      //TODO sort by window resolution, ts with same frame can be optimized
      final Storage storage = getStorage(archive);
      final Collection<Consolidator> consolidators = archive.getConsolidators();
      for (final SamplingWindow samplingWindow : archive.getSamplingWindows()) {
        accumulate(timestamp, value, consolidators);

        final long beginning = beginningFor(archive, firstTimestamp);
        final long currentWindowId = windowId(beginning, timestamp, samplingWindow.getDuration());
        final long previousWindowId = windowId(beginning, previousTimestamp, samplingWindow.getDuration());
        if (previousTimestamp != null && currentWindowId != previousWindowId) {
          //previousTimestamp will be null on first run with empty archives
          final long previousWindowBeginning = beginning + previousWindowId * samplingWindow.getDuration();
          persist(previousWindowBeginning, storage, consolidators);
        }
      }
    }
  }

}