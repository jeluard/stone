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
import com.github.jeluard.stone.helper.Loggers;
import com.google.common.base.Supplier;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Level;

/**
 * Base implementation for {@link StorageFactory}.
 */
public abstract class BaseStorageFactory<T extends Storage> implements StorageFactory<T> {

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

  /**
   * @param id
   * @param archive
   * @param window
   * @return an initialized {@link Storage} dedicated to {@code id}/{@code archive}/{@code window}
   * @throws IOException 
   */
  protected abstract T create(String id, Archive archive, Window window) throws IOException;

  /**
   * @return all currently created {@link Storage}s
   */
  protected final Iterable<T> getStorages() {
    return this.cache.values();
  }

  /**
   * Optionally close a {@link Storage}.
   *
   * @param storage
   * @throws IOException 
   */
  protected void close(T storage) throws IOException {
  }

  @Override
  public void close(final String id, final Archive archive, final Window window) throws IOException {
    final T storage = this.cache.remove(new Triple<String, Archive, Window>(id, archive, window));
    if (storage == null) {
      if (Loggers.BASE_LOGGER.isLoggable(Level.WARNING)) {
        Loggers.BASE_LOGGER.log(Level.WARNING, "{0} for <{1}, {2}, {3}> does not exist", new Object[]{Storage.class.getSimpleName(), id, storage, window});
      }
      return;
    }

    close(storage);
  }

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
        close(storage);
      } catch (IOException e) {
        if (Loggers.BASE_LOGGER.isLoggable(Level.WARNING)) {
          Loggers.BASE_LOGGER.log(Level.WARNING, "Got an exception while cleaning <"+storage+">", e);
        }
      }
    }
    this.cache.clear();
  }

  /**
   * Optionnally perform extra cleanup.
   *
   * @throws IOException 
   * @see #close()
   */
  protected void cleanup() throws IOException {
  }
  
}