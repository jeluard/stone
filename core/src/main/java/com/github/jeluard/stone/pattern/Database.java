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
package com.github.jeluard.stone.pattern;

import com.github.jeluard.guayaba.annotation.Idempotent;
import com.github.jeluard.stone.api.ConsolidationListener;
import com.github.jeluard.stone.api.TimeSeries;
import com.github.jeluard.stone.api.Window;
import com.github.jeluard.stone.api.WindowedTimeSeries;
import com.github.jeluard.stone.helper.Loggers;
import com.github.jeluard.stone.helper.Storages;
import com.github.jeluard.stone.spi.Dispatcher;
import com.github.jeluard.stone.spi.Storage;
import com.github.jeluard.stone.spi.StorageFactory;
import com.google.common.base.Preconditions;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Level;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Main entry point to manage {@link TimeSeries} life cycle.
 */
@ThreadSafe
public final class Database implements Closeable {

  private static final int DEFAULT_GRANULARITY = 1;

  private final Dispatcher dispatcher;
  private final StorageFactory<?> storageFactory;
  private final ConcurrentMap<String, WindowedTimeSeries> timeSeriess = new ConcurrentHashMap<String, WindowedTimeSeries>();

  public Database(final Dispatcher dispatcher, final StorageFactory<?> storageFactory) {
    this.dispatcher = Preconditions.checkNotNull(dispatcher, "null dispatcher");
    this.storageFactory = Preconditions.checkNotNull(storageFactory, "null storageFactory");
  }

  private Storage createStorage(final String id, final int granularity, final long duration) throws IOException {
    return this.storageFactory.createOrGet(id, granularity, duration);
  }

  private Window enrichWindow(final Window window, final Storage storage) {
    final List<ConsolidationListener> consolidationListeners = new ArrayList<ConsolidationListener>(1+window.getConsolidationListeners().size());
    consolidationListeners.add(Storages.asConsolidationListener(storage, Loggers.BASE_LOGGER));
    consolidationListeners.addAll(window.getConsolidationListeners());
    return Window.of(window.getSize()).listenedBy(consolidationListeners.toArray(new ConsolidationListener[consolidationListeners.size()])).consolidatedBy(window.getConsolidatorTypes().toArray(new Class[window.getConsolidatorTypes().size()]));
  }

  private List<Window> enrichWindows(final Window[] windows, final Storage storage) {
    final List<Window> enricherWindows = new ArrayList<Window>(windows.length);
    for (final Window window : windows) {
      enricherWindows.add(enrichWindow(window, storage));
    }
    return enricherWindows;
  }

  /**
   * Create a {@link WindowedTimeSeries} with a default granularity of {@code Database#DEFAULT_GRANULARITY}.
   *
   * @param id
   * @param duration
   * @param windows
   * @return
   * @throws IOException
   * @see #createOrOpen(java.lang.String, int, com.github.jeluard.stone.api.Window[])
   */
  public WindowedTimeSeries createOrOpen(final String id, final long duration, final Window ... windows) throws IOException {
    return createOrOpen(id, Database.DEFAULT_GRANULARITY, duration, windows);
  }

  /**
   * Create a {@link WindowedTimeSeries} using this database {@link Dispatcher} and {@link StorageFactory}.
   * <br>
   * During the life cycle of this {@link Database} a uniquely identified {@link TimeSeries} can be opened only once.
   * Calling this method twice with a same value for {@code id} will fail.
   *
   * @param id
   * @param granularity
   * @param windows
   * @return
   * @throws IOException 
   */
  public WindowedTimeSeries createOrOpen(final String id, final int granularity, final long duration, final Window ... windows) throws IOException {
    Preconditions.checkNotNull(id, "null id");
    Preconditions.checkNotNull(windows, "null windows");

    final Storage storage = createStorage(id, granularity, duration);
    final WindowedTimeSeries timeSeries = new WindowedTimeSeries(id, granularity, enrichWindows(windows, storage), this.dispatcher) {
      @Override
      protected void cleanup() throws IOException {
        super.cleanup();

        Database.this.close(id);
      }
    };

    //We can't have two TimeSeries with same id as TimeSeries enforce this.
    this.timeSeriess.putIfAbsent(id, timeSeries);

    return timeSeries;
  }

  private void close(final TimeSeries timeSeries) throws IOException {
    timeSeries.close();
    this.storageFactory.close(timeSeries.getId());
  }

  public void close(final String id) throws IOException {
    Preconditions.checkNotNull(id, "null id");

    final TimeSeries timeSeries = this.timeSeriess.remove(id);
    if (timeSeries != null) {
      close(timeSeries);
    }
  }

  /**
   * Remove and close all currently used {@link WindowedTimeSeries}. Do not delete any data.
   * <br>
   * Any previously created {@link TimeSeries} will be unusable and will have to be re-created.
   */
  @Idempotent
  @Override
  public void close() {
    for (final TimeSeries timeSeries : this.timeSeriess.values()) {
      try {
        close(timeSeries);
      } catch (IOException e) {
        if (Loggers.BASE_LOGGER.isLoggable(Level.WARNING)) {
          Loggers.BASE_LOGGER.log(Level.WARNING, "Got exception while closing <"+timeSeries+">", e);
        }
      }
    }
  }

}