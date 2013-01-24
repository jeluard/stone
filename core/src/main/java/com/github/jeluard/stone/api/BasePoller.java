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
package com.github.jeluard.stone.api;

import com.github.jeluard.guayaba.annotation.Idempotent;
import com.github.jeluard.guayaba.lang.Cancelable;
import com.github.jeluard.guayaba.util.concurrent.Scheduler;
import com.github.jeluard.stone.helper.Loggers;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

import org.joda.time.Duration;

/**
 * Base implementation tracking a metric for a collection of resources <T> periodically.
 */
public abstract class BasePoller<T> implements Cancelable {

  private final Database database;
  private final Duration period;
  private final Iterable<T> ts;
  private final Collection<Archive> archives;
  private final Map<T, TimeSeries> timeseriess;
  private final Scheduler scheduler;

  public BasePoller(final Database database, final Iterable<T> ts, final Duration period, final Collection<Archive> archives, final ExecutorService schedulerExecutorService) throws IOException {
    this.database = Preconditions.checkNotNull(database, "null database");
    this.period = Preconditions.checkNotNull(period, "null period");
    this.ts = Preconditions.checkNotNull(ts, "null ts");
    this.archives = Preconditions.checkNotNull(archives, "null archives");
    this.timeseriess = createTimeSeries(ts);
    this.scheduler = new Scheduler(period.getMillis(), TimeUnit.MILLISECONDS, schedulerExecutorService, Loggers.BASE_LOGGER);
  }

  public BasePoller(final Database database, final Iterable<T> ts, final Duration period, final Collection<Archive> archives) throws IOException {
    this(database, ts, period, archives, Scheduler.defaultExecutorService(1, Loggers.BASE_LOGGER));
  }

  private Map<T, TimeSeries> createTimeSeries(final Iterable<T> ts) throws IOException {
    final Map<T, TimeSeries> map = new HashMap<T, TimeSeries>();
    for (final T t : ts) {
      final String id = id(t);
      final Collection<ConsolidationListener> consolidationListeners = createConsolidationListeners(t, id).or(Collections.<ConsolidationListener>emptyList());
      map.put(t, this.database.createOrOpen(id, this.period, this.archives, consolidationListeners));
    }
    return map;
  }

  protected Optional<Collection<ConsolidationListener>> createConsolidationListeners(final T t, final String id) {
    return Optional.absent();
  }

  /**
   * Start the polling process.
   */
  @Idempotent
  public final void start() {
    //TODO Make sure this idempotent
    this.scheduler.schedule(new Runnable() {
      @Override
      public void run() {
        for (final T t : BasePoller.this.ts) {
          final long timestamp = System.currentTimeMillis();
          try {
            final int metric = metric(t);
            try {
              BasePoller.this.timeseriess.get(t).publish(timestamp, metric);
            } catch (IOException e) {
              if (e instanceof InterruptedIOException) {
                if (Loggers.BASE_LOGGER.isLoggable(Level.FINEST)) {
                  Loggers.BASE_LOGGER.log(Level.FINEST, "Interrupted while publishing for <"+t+">", e);
                }
                return;
              }

              if (Loggers.BASE_LOGGER.isLoggable(Level.WARNING)) {
                Loggers.BASE_LOGGER.log(Level.WARNING, "Got exception while publishing for <"+t+">", e);
              }
            }
          } catch (Exception e) {
            if (Loggers.BASE_LOGGER.isLoggable(Level.WARNING)) {
              Loggers.BASE_LOGGER.log(Level.WARNING, "Got exception while accessing metric for <"+t+">", e);
            }
          }
        }
      }
      @Override
      public String toString() {
        return "Poller";
      }
    });
  }

  /**
   * @param t
   * @return a unique id for this {@link TimeSeries}
   */
  protected abstract String id(T t);

  /**
   * @param t
   * @return the extracted metric for {@link TimeSeries}
   * @throws Exception 
   */
  protected abstract int metric(T t) throws Exception;

  public final Map<String, Map<Window, Reader>> getReaders() {
    return null;//TODO
  }

  @Override
  public final void cancel() {
    this.scheduler.cancel();
    try {
      this.database.close();
    } catch (IOException e) {
      if (Loggers.BASE_LOGGER.isLoggable(Level.WARNING)) {
        Loggers.BASE_LOGGER.log(Level.WARNING, "Got exception while publishing for <"+ts+">", e);
      }
    }
  }

}