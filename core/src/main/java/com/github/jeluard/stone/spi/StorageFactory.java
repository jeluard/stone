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
package com.github.jeluard.stone.spi;

import com.github.jeluard.guayaba.annotation.Idempotent;
import com.github.jeluard.guayaba.base.Pair;
import com.github.jeluard.guayaba.util.concurrent.ConcurrentMaps;
import com.github.jeluard.stone.api.Window;
import com.github.jeluard.stone.helper.Loggers;
import com.google.common.base.Supplier;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Level;
import javax.annotation.concurrent.ThreadSafe;

/**
 * Abstract creation of {@link Storage} backend per persistent {@link Window}.
 * <br />
 * Common structure can then be shared accross {@link Storage}.
 */
@ThreadSafe
public abstract class StorageFactory<T extends Storage> implements Closeable {

  private final ConcurrentMap<Pair<String, Window>, T> cache = new ConcurrentHashMap<Pair<String, Window>, T>();

  /**
   * Create or open a {@link Storage} specific to provided {@code id}, {@link Archive} and {@link Window}.
   * This {@link Storage} will then only be used to persist associated data.
   * Internal resources can be shared but {@link Storage} methods should be isolated to others {@code id}, {@link Archive} and {@link Window}.
   * <br>
   * At this stage {@link Storage} is initialized and ready to be used.
   * <br>
   * No caching should be done here as this is done by the caller.
   *
   * @param id unique id of associated {@link com.github.jeluard.stone.api.TimeSeries}
   * @param window associated {@link Window}
   * @return a fully initialized {@link Storage}
   * @throws IOException 
   */
  public final T createOrGet(final String id, final Window window) throws IOException {
    return ConcurrentMaps.putIfAbsentAndReturn(this.cache, new Pair<String, Window>(id, window), new Supplier<T>() {
      @Override
      public T get() {
        try {
          return create(id, window);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    });
  }

  /**
   * @param id
   * @param window
   * @return an initialized {@link Storage} dedicated to {@code id}/{@code window}/{@code duration}
   * @throws IOException 
   */
  protected abstract T create(String id, Window window) throws IOException;

  /**
   * @return all currently created {@link Storage}s
   */
  protected final Iterable<T> getStorages() {
    return this.cache.values();
  }

  /**
   * {@inheritDoc}
   */
  @Idempotent
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
   * Close the {@link Storage} created for this triple.
   *
   * @param id
   * @param window
   * @throws IOException 
   */
  @Idempotent
  public final void close(final String id, final Window window) throws IOException {
    final T storage = this.cache.remove(new Pair<String, Window>(id, window));
    if (storage == null) {
      if (Loggers.BASE_LOGGER.isLoggable(Level.WARNING)) {
        Loggers.BASE_LOGGER.log(Level.WARNING, "{0} for <{1}, {2}> does not exist", new Object[]{Storage.class.getSimpleName(), id, window});
      }
      return;
    }

    close(storage);
  }

  /**
   * {@link Closeable#close()} a {@link Storage} if it's an instance of {@link Closeable}.
   *
   * @param storage
   * @throws IOException 
   */
  protected final void close(T storage) throws IOException {
    if (storage instanceof Closeable) {
      Closeable.class.cast(storage).close();
    }
  }

  /**
   * Optionnally perform extra cleanup.
   *
   * @throws IOException 
   * @see #close()
   */
  protected void cleanup() throws IOException {
  }

  /**
   * Optionnaly delete all {@link Storage} generated resources associated to timeseries {@code id}.
   *
   * @param id
   * @throws IOException 
   */
  @Idempotent
  public void delete(String id) throws IOException {
  }

}