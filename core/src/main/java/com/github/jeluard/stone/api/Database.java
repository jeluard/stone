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
import com.google.common.base.Preconditions;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.joda.time.Duration;

/**
 * Main entry point to manage {@link TimeSeries} life cycle.
 */
public final class Database {

  private static final Duration DEFAULT_GRANULARITY = Duration.millis(1L);

  //TODO separate create/open/createOrOpen/close/delete
  private final Engine engine;
  private final ConcurrentMap<String, TimeSeries> timeSeriess = new ConcurrentHashMap<String, TimeSeries>();

  public Database(final StorageFactory<?> storageFactory) throws IOException {
    this.engine = new Engine(Preconditions.checkNotNull(storageFactory, "null storageFactory"));
  }

  public TimeSeries create(final String id, final Collection<Archive> archives) throws IOException {
    return create(id, archives, Collections.<ConsolidationListener>emptyList());
  }

  public TimeSeries create(final String id, final Collection<Archive> archives, final Collection<ConsolidationListener> consolidationListeners) throws IOException {
    return create(id, Database.DEFAULT_GRANULARITY, archives, consolidationListeners);
  }

  /**
   * 
   *
   * @param id
   * @param granularity
   * @param archives
   * @param consolidationListeners
   * @return
   * @throws IOException 
   */
  public TimeSeries create(final String id, final Duration granularity, final Collection<Archive> archives, final Collection<ConsolidationListener> consolidationListeners) throws IOException {
    Preconditions.checkNotNull(id, "null id");
    Preconditions.checkNotNull(archives, "null archives");
    Preconditions.checkNotNull(consolidationListeners, "null consolidationListeners");

    final TimeSeries timeSeries = new TimeSeries(id, granularity, archives, consolidationListeners, this.engine);
    if (this.timeSeriess.putIfAbsent(id, timeSeries) != null) {
      throw new IllegalArgumentException("A "+TimeSeries.class.getSimpleName()+" with id <"+id+"> already exists");
    }
    return timeSeries;
  }

  @Idempotent
  public void close(final String id) throws IOException {
    Preconditions.checkNotNull(id, "null id");

    final TimeSeries timeSeries = this.timeSeriess.remove(id);
    if (timeSeries == null) {
      
    }
//    timeSeries.close();
  }

  @Idempotent
  public void close() {
  }

}