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
import com.github.jeluard.stone.api.Window;
import com.github.jeluard.stone.spi.Consolidator;
import com.github.jeluard.stone.spi.Dispatcher;
import com.github.jeluard.stone.spi.Storage;
import com.github.jeluard.stone.spi.StorageFactory;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import java.io.Closeable;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 */
public class Engine implements Closeable {

  private static final Logger LOGGER = Logger.getLogger("com.github.jeluard.stone");

  private final Dispatcher dispatcher;
  private final Triple<Window, Storage, Consolidator[]>[] triples;
  private Map<Pair<Archive, Window>, Pair<Storage, Consolidator[]>> stuffs = new HashMap<Pair<Archive, Window>, Pair<Storage, Consolidator[]>>();

  public Engine(final String id, final Collection<Archive> archives, final Dispatcher dispatcher, final StorageFactory storageFactory) throws IOException {
    Preconditions2.checkNotEmpty(archives, "null archives").toArray(new Archive[archives.size()]);
    this.dispatcher = Preconditions.checkNotNull(dispatcher, "null dispatcher");
    this.stuffs.putAll(createStorages(storageFactory, id, archives));
    this.triples = new Triple[this.stuffs.size()];
    for (Iterables2.Indexed<Map.Entry<Pair<Archive, Window>, Pair<Storage, Consolidator[]>>> stuff : Iterables2.withIndex(this.stuffs.entrySet())) {
      this.triples[stuff.index] = new Triple<Window, Storage, Consolidator[]>(stuff.value.getKey().second, stuff.value.getValue().first, stuff.value.getValue().second);
    }
  }

  private Storage createStorage(final StorageFactory storageFactory, final String id, final Archive archive, final Window window) throws IOException {
    return storageFactory.createOrOpen(id, archive, window);
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

  public Map<Pair<Archive, Window>, Storage> getStorages() {
    return Maps.transformValues(this.stuffs, new Function<Pair<Storage, Consolidator[]>, Storage>() {
      @Override
      public Storage apply(final Pair<Storage, Consolidator[]> input) {
        return input.first;
      }
    });
  }

  private long windowId(final long beginning, final long timestamp, final long duration) {
    return (timestamp - beginning) / duration;
  }

  private void accumulate(final long timestamp, final int value, final Consolidator[] consolidators) {
    this.dispatcher.accumulate(timestamp, value, consolidators);
  }

  private void persist(final long timestamp, final Storage storage, final Consolidator[] consolidators) throws IOException {
    storage.append(timestamp, this.dispatcher.reduce(consolidators));
  }

  public void publish(final long beginningTimestamp, final long previousTimestamp, final long currentTimestamp, final int value) throws IOException {
    for (final Triple<Window, Storage, Consolidator[]> triple : this.triples) {
      accumulate(currentTimestamp, value, triple.third);

      //previousTimestamp == 0 if this is the first publish call and associated storage was empty (or new)
      if (previousTimestamp != 0L) {
        final long duration = triple.first.getResolution().getMillis();
        final long currentWindowId = windowId(beginningTimestamp, currentTimestamp, duration);
        final long previousWindowId = windowId(beginningTimestamp, previousTimestamp, duration);
        if (currentWindowId != previousWindowId) {
          final long previousWindowBeginning = beginningTimestamp + previousWindowId * duration;

          persist(previousWindowBeginning, triple.second, triple.third);
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
    for (final Storage storage : getStorages().values()) {
      try {
        if (storage instanceof Closeable) {
          Closeable.class.cast(storage).close();
        }
      } catch (IOException e) {
        if (Engine.LOGGER.isLoggable(Level.WARNING)) {
          Engine.LOGGER.log(Level.WARNING, "Got an exception while closing <"+storage+">", e);
        }
      }
    }
  }

}