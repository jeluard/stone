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
import com.github.jeluard.guayaba.base.Triple;
import com.github.jeluard.guayaba.lang.Iterables2;
import com.github.jeluard.stone.impl.Engine;
import com.github.jeluard.stone.spi.Storage;
import com.github.jeluard.stone.spi.StorageFactory;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.Collections2;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.joda.time.DateTime;
import org.joda.time.Interval;

public final class TimeSeries {

  private final String id;
  private final ConsolidationListener[] consolidationListeners;
  private final Engine engine;
  private final Triple<Window, Storage, Consolidator[]>[] triples;
  private long beginning;
  private long latest;

  TimeSeries(final String id, final Collection<Archive> archives, final Collection<ConsolidationListener> consolidationListeners, final Engine engine) throws IOException {
    this.id = Preconditions.checkNotNull(id, "null id");
    this.consolidationListeners = Preconditions.checkNotNull(consolidationListeners, "null consolidationListeners").toArray(new ConsolidationListener[consolidationListeners.size()]);
    this.engine = Preconditions.checkNotNull(engine, "null engine");
    final Map<Pair<Archive, Window>, Pair<Storage, Consolidator[]>> stuffs = new HashMap<Pair<Archive, Window>, Pair<Storage, Consolidator[]>>();
    stuffs.putAll(createStorages(engine.getStorageFactory(), id, archives));
    this.triples = new Triple[stuffs.size()];
    for (final Iterables2.Indexed<Map.Entry<Pair<Archive, Window>, Pair<Storage, Consolidator[]>>> stuff : Iterables2.withIndex(stuffs.entrySet())) {
      this.triples[stuff.index] = new Triple<Window, Storage, Consolidator[]>(stuff.value.getKey().second, stuff.value.getValue().first, stuff.value.getValue().second);
    }

    final Interval initialSpan = extractInterval(Collections2.transform(Arrays.asList(this.triples), new Function<Triple<Window, Storage, Consolidator[]>, Storage>() {
      @Override
      public Storage apply(Triple<Window, Storage, Consolidator[]> input) {
        return input.second;
      }
    }));
    this.beginning = initialSpan.getStartMillis();
    this.latest = initialSpan.getEndMillis();
  }

  private Consolidator createConsolidator(final Class<? extends Consolidator> type) {
    try {
      //TODO add support for Consolidator(int maxSamples)
      try {
        final Constructor<? extends Consolidator> defaultConstructor = type.getConstructor();
        return defaultConstructor.newInstance();
      } catch (NoSuchMethodException e) {
        throw new IllegalArgumentException("Failed to find default constructor for "+type.getCanonicalName());
      }
    } catch (ReflectiveOperationException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * @param archive
   * @return all {@link Consolidator} mapping to {@link Archive#getConsolidators()}
   * @see #createConsolidator(java.lang.Class)
   */
  private Consolidator[] createConsolidators(final Archive archive) {
    final Collection<Class<? extends Consolidator>> types = archive.getConsolidators();
    final Consolidator[] consolidators = new Consolidator[types.size()];
    for (final Iterables2.Indexed<Class<? extends Consolidator>> indexedType : Iterables2.withIndex(types)) {
      consolidators[indexedType.index] = createConsolidator(indexedType.value);
    }
    return consolidators;
  }

  private Map<Pair<Archive, Window>, Pair<Storage, Consolidator[]>> createStorages(final StorageFactory storageFactory, final String id, final Collection<Archive> archives) throws IOException {
    final Map<Pair<Archive, Window>, Pair<Storage, Consolidator[]>> newStorages = new HashMap<Pair<Archive, Window>, Pair<Storage, Consolidator[]>>();
    for (final Archive archive : archives) {
      for (final Window window : archive.getWindows()) {
        newStorages.put(new Pair<Archive, Window>(archive, window), new Pair<Storage, Consolidator[]>(createStorage(storageFactory, id, archive, window), createConsolidators(archive)));
      }
    }
    return newStorages;
  }

  private Storage createStorage(final StorageFactory storageFactory, final String id, final Archive archive, final Window window) throws IOException {
    return storageFactory.createOrGet(id, archive, window);
  }

  private Interval extractInterval(final Collection<Storage> storages) throws IOException {
    return new Interval(extractBeginning(storages.iterator().next()), extractLatest(storages));
  }

  /**
   * @param storage
   * @return beginning of {@link Storage#beginning()} if any; 0L otherwise
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
   * @return latest (more recent) timestamp stored in specified {@link archives}; 0L if all archive are empty
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

  private void checkNotBeforeLatestTimestamp(final long previousTimestamp, final long currentTimestamp) {
    if (!(currentTimestamp > previousTimestamp)) {
      throw new IllegalArgumentException("Provided timestamp from <"+currentTimestamp+"> must be more recent than <"+previousTimestamp+">");
    }
  }

  private long recordLatest(final long timestamp) {
    //Set to the new timestamp if value is null (i.e. no value as yet been recorded)
    final long previousTimestamp = this.latest;
    this.latest = timestamp;
    checkNotBeforeLatestTimestamp(previousTimestamp, timestamp);
    return previousTimestamp;
  }

  private long inferBeginning(final long timestamp) {
    //If beginning is still null (i.e. all storages where empty) sets it's value to timestamp
    //This will be done only once (hence track the first timestamp received)
    if (this.beginning == 0L) {
      this.beginning = timestamp;
    }
    return this.beginning;
  }

  public void publish(final long timestamp, final int value) throws IOException {
    Preconditions.checkNotNull(timestamp, "null timestamp");

    final long previousTimestamp = recordLatest(timestamp);
    final long beginningTimestamp = inferBeginning(timestamp);

    this.engine.publish(this.triples, this.consolidationListeners, beginningTimestamp, previousTimestamp, timestamp, value);
  }

}