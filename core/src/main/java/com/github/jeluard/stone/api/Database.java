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

import com.github.jeluard.stone.spi.Dispatcher;
import com.github.jeluard.stone.spi.StorageFactory;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import javax.annotation.concurrent.ThreadSafe;

import org.joda.time.Duration;

/**
 * Main entry point to manage {@link TimeSeries} life cycle.
 */
@ThreadSafe
public final class Database {

  private static final Duration DEFAULT_GRANULARITY = Duration.millis(1L);

  private final StorageFactory<?> storageFactory;
  private final Dispatcher dispatcher;
  private final ConcurrentMap<String, TimeSeries> timeSeriess = new ConcurrentHashMap<String, TimeSeries>();

  public Database(final Dispatcher dispatcher, final StorageFactory<?> storageFactory) {
    this.dispatcher = Preconditions.checkNotNull(dispatcher, "null dispatcher");
    this.storageFactory = Preconditions.checkNotNull(storageFactory, "null storageFactory");
  }

  /**
   * Create a {@link TimeSeries} with a default granularity of {@code Database#DEFAULT_GRANULARITY}.
   *
   * @param id
   * @param windows
   * @return
   * @throws IOException
   * @see #createOrOpen(java.lang.String, org.joda.time.Duration, com.github.jeluard.stone.api.Window[])
   */
  public TimeSeries createOrOpen(final String id, final Window ... windows) throws IOException {
    return createOrOpen(id, Database.DEFAULT_GRANULARITY, windows);
  }

  /**
   * Create a {@link TimeSeries} using this database {@link Dispatcher} and {@link StorageFactory}.
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
  public TimeSeries createOrOpen(final String id, final Duration granularity, final Window ... windows) throws IOException {
    Preconditions.checkNotNull(id, "null id");
    Preconditions.checkNotNull(granularity, "null granularity");
    Preconditions.checkNotNull(windows, "null windows");

    final TimeSeries timeSeries = new TimeSeries(id, granularity, windows, this.dispatcher, this.storageFactory) {
      @Override
      protected void cleanup() {
        Database.this.timeSeriess.remove(id);
      }
    };

    //If We can't have two TimeSeries with same id as TimeSeries enforce this.
    this.timeSeriess.putIfAbsent(id, timeSeries);

    return timeSeries;
  }

  private Optional<TimeSeries> remove(final String id) {
    return Optional.fromNullable(this.timeSeriess.remove(id));
  }

  /**
   * Remove and close all currently used {@link TimeSeries}. Do not delete any data.
   * <br>
   * Any previously created {@link TimeSeries} will be unusable and will have to be re-created.
   */
  public void reset() {
    for (final TimeSeries timeSeries : this.timeSeriess.values()) {
      timeSeries.close();
    }
  }

}