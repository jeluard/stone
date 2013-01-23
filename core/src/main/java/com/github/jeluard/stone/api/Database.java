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

import com.github.jeluard.guayaba.annotation.Idempotent;
import com.github.jeluard.stone.impl.Engine;
import com.github.jeluard.stone.spi.StorageFactory;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.joda.time.Duration;

/**
 * Main entry point to manage {@link TimeSeries} life cycle.
 */
public final class Database implements Closeable {

  private static final Duration DEFAULT_GRANULARITY = Duration.millis(1L);

  final StorageFactory<?> storageFactory;
  final Engine engine = new Engine();
  private final ConcurrentMap<String, TimeSeries> timeSeriess = new ConcurrentHashMap<String, TimeSeries>();

  public Database(final StorageFactory<?> storageFactory) throws IOException {
    this.storageFactory = Preconditions.checkNotNull(storageFactory, "null storageFactory");
  }

  public TimeSeries createOrOpen(final String id, final Collection<Archive> archives) throws IOException {
    return createOrOpen(id, archives, Collections.<ConsolidationListener>emptyList());
  }

  public TimeSeries createOrOpen(final String id, final Collection<Archive> archives, final Collection<ConsolidationListener> consolidationListeners) throws IOException {
    return createOrOpen(id, Database.DEFAULT_GRANULARITY, archives, consolidationListeners);
  }

  /**
   * 
   * During the life cycle of this {@link Database} a uniquely identified {@link TimeSeries} can be opened only once.
   * Calling this method twice with a same value for {@code id} will fail.
   *
   * @param id
   * @param granularity
   * @param archives
   * @param consolidationListeners
   * @return
   * @throws IOException 
   */
  public TimeSeries createOrOpen(final String id, final Duration granularity, final Collection<Archive> archives, final Collection<ConsolidationListener> consolidationListeners) throws IOException {
    Preconditions.checkNotNull(id, "null id");
    Preconditions.checkNotNull(granularity, "null granularity");
    Preconditions.checkNotNull(archives, "null archives");
    Preconditions.checkNotNull(consolidationListeners, "null consolidationListeners");

    final TimeSeries timeSeries = new TimeSeries(id, granularity, archives, consolidationListeners, this);
    if (this.timeSeriess.putIfAbsent(id, timeSeries) != null) {
      throw new IllegalArgumentException("A "+TimeSeries.class.getSimpleName()+" with id <"+id+"> already exists");
    }
    return timeSeries;
  }

  Optional<TimeSeries> remove(final TimeSeries timeSeries) {
    return Optional.fromNullable(this.timeSeriess.remove(timeSeries.getId()));
  }

  @Idempotent
  @Override
  public void close() throws IOException {
    this.timeSeriess.clear();
    this.storageFactory.close();
  }

}