/*
 * Copyright 2013 julien.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.jeluard.stone.impl;

import com.github.jeluard.guayaba.base.Pair;
import com.github.jeluard.guayaba.base.Preconditions2;
import com.github.jeluard.guayaba.base.Triple;
import com.github.jeluard.guayaba.lang.Iterables2;
import com.github.jeluard.stone.api.Archive;
import com.github.jeluard.stone.api.Consolidator;
import com.github.jeluard.stone.api.Window;
import com.github.jeluard.stone.spi.Storage;
import com.github.jeluard.stone.spi.StorageFactory;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.joda.time.DateTime;
import org.joda.time.Interval;

/**
 *
 */
public final class Engine implements Closeable {

  private final StorageFactory<?> storageFactory;
  private final Triple<Window, Storage, Consolidator[]>[] triples;
  private Map<Pair<Archive, Window>, Pair<Storage, Consolidator[]>> stuffs = new HashMap<Pair<Archive, Window>, Pair<Storage, Consolidator[]>>();
  private final Interval span;

  public Engine(final String id, final Collection<Archive> archives, final StorageFactory storageFactory) throws IOException {
    Preconditions2.checkNotEmpty(archives, "null archives");
    this.storageFactory = Preconditions.checkNotNull(storageFactory, "null storageFactory");
    this.stuffs.putAll(createStorages(storageFactory, id, archives));
    this.triples = new Triple[this.stuffs.size()];
    for (final Iterables2.Indexed<Map.Entry<Pair<Archive, Window>, Pair<Storage, Consolidator[]>>> stuff : Iterables2.withIndex(this.stuffs.entrySet())) {
      this.triples[stuff.index] = new Triple<Window, Storage, Consolidator[]>(stuff.value.getKey().second, stuff.value.getValue().first, stuff.value.getValue().second);
    }
    this.span = new Interval(extractBeginning(this.triples[0].second), extractLatest(this.triples));
  }

  public Interval span() {
    return this.span;
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
   * @param triples
   * @return latest (more recent) timestamp stored in specified {@link archives}; null if all archive are empty
   * @throws IOException 
   */
  private long extractLatest(final Triple<Window, Storage, Consolidator[]>[] triples) throws IOException {
    long storageLatest = 0L;
    for (final Triple<Window, Storage, Consolidator[]> triple : triples) {
      final Optional<DateTime> optionalInterval = triple.second.end(); 
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

  private Storage createStorage(final StorageFactory storageFactory, final String id, final Archive archive, final Window window) throws IOException {
    return storageFactory.createOrGet(id, archive, window);
  }

  private Consolidator createConsolidator(final Class<? extends Consolidator> type) {
    try {
      return type.newInstance();
    } catch (ReflectiveOperationException e) {
      throw new RuntimeException(e);
    }
  }

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

  private long windowId(final long beginning, final long timestamp, final long duration) {
    return (timestamp - beginning) / duration;
  }

  private void accumulate(final Consolidator[] consolidators, final long timestamp, final int value) {
    for (final Consolidator consolidator : consolidators) {
      consolidator.accumulate(timestamp, value);
    }
  }

  private void persist(final Consolidator[] consolidators, final long timestamp, final Storage storage) throws IOException {
    //TODO Do not create arrays each time?
    final int[] integers = new int[consolidators.length];
    for (int i = 0; i < consolidators.length; i++) {
      integers[i] = consolidators[i].consolidateAndReset();
    }
    storage.append(timestamp, integers);
  }

  public void publish(final long beginningTimestamp, final long previousTimestamp, final long currentTimestamp, final int value) throws IOException {
    for (final Triple<Window, Storage, Consolidator[]> triple : this.triples) {
      accumulate(triple.third, currentTimestamp, value);

      //previousTimestamp == 0 if this is the first publish call and associated storage was empty (or new)
      if (previousTimestamp != 0L) {
        final long duration = triple.first.getResolution().getMillis();
        final long currentWindowId = windowId(beginningTimestamp, currentTimestamp, duration);
        final long previousWindowId = windowId(beginningTimestamp, previousTimestamp, duration);
        if (currentWindowId != previousWindowId) {
          final long previousWindowBeginning = beginningTimestamp + previousWindowId * duration;

          persist(triple.third, previousWindowBeginning, triple.second);
        }
      }
    }
  }

  /**
   * Delegate to {@link StorageFactory#close()}.
   *
   * @throws IOException 
   */
  @Override
  public void close() throws IOException {
    this.storageFactory.close();
  }

}