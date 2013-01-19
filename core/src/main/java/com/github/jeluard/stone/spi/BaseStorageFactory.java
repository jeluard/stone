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
package com.github.jeluard.stone.spi;

import com.github.jeluard.guayaba.base.Triple;
import com.github.jeluard.guayaba.util.concurrent.ConcurrentMaps;
import com.github.jeluard.stone.api.Archive;
import com.github.jeluard.stone.api.Window;
import com.google.common.base.Supplier;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Base implementation for {@link StorageFactory}.
 */
public abstract class BaseStorageFactory<T extends Storage> implements StorageFactory<T> {

  private static final Logger LOGGER = Logger.getLogger("com.github.jeluard.stone");

  private final ConcurrentMap<Triple<String, Archive, Window>, T> cache = new ConcurrentHashMap<Triple<String, Archive, Window>, T>();

  @Override
  public final T createOrGet(final String id, final Archive archive, final Window window) throws IOException {
    return ConcurrentMaps.putIfAbsentAndReturn(this.cache, new Triple<String, Archive, Window>(id, archive, window), new Supplier<T>() {
      @Override
      public T get() {
        try {
          return create(id, archive, window);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    });
  }

  protected abstract T create(String id, Archive archive, Window window) throws IOException;

  protected Iterable<T> getStorages() {
    return this.cache.values();
  }

  protected abstract void cleanup(T storage) throws IOException;

  /**
   * Call {@link #cleanup()} on all {@link Storage}.
   *
   * @throws IOException 
   */
  @Override
  public final void close() throws IOException {
    cleanup();

    for (final T storage : this.cache.values()) {
      try {
        cleanup(storage);
      } catch (IOException e) {
        if (BaseStorageFactory.LOGGER.isLoggable(Level.WARNING)) {
          BaseStorageFactory.LOGGER.log(Level.WARNING, "Got an exception while cleaning <"+storage+">", e);
        }
      }
    }
    this.cache.clear();
  }

  protected abstract void cleanup();
  
}