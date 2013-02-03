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
import com.github.jeluard.guayaba.lang.Cancelable;
import com.github.jeluard.guayaba.util.concurrent.Scheduler;
import com.github.jeluard.stone.helper.Loggers;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

import org.joda.time.Duration;

/**
 * Base implementation tracking a metric for a collection of resources <T> periodically.
 */
public abstract class BasePoller<T> implements Cancelable {

  class Publisher implements Runnable {

    @Override
    public void run() {
      
    }
    
  }

  private final Database database;
  private final Duration period;
  private final Iterable<T> ts;
  private final Window[] windows;
  private final Map<T, TimeSeries> timeseriess;
  private final Scheduler scheduler;

  public BasePoller(final Database database, final Iterable<T> ts, final Duration period, final Collection<Window> windows, final ExecutorService schedulerExecutorService) throws IOException {
    this.database = Preconditions.checkNotNull(database, "null database");
    this.period = Preconditions.checkNotNull(period, "null period");
    this.ts = Preconditions.checkNotNull(ts, "null ts");
    this.windows = Preconditions.checkNotNull(windows, "null windows").toArray(new Window[windows.size()]);
    this.timeseriess = createTimeSeries(ts);
    this.scheduler = new Scheduler(period.getMillis(), TimeUnit.MILLISECONDS, schedulerExecutorService, Loggers.BASE_LOGGER);
  }

  public BasePoller(final Database database, final Iterable<T> ts, final Duration period, final Collection<Window> windows) throws IOException {
    this(database, ts, period, windows, Scheduler.defaultExecutorService(1, Loggers.BASE_LOGGER));
  }

  private Map<T, TimeSeries> createTimeSeries(final Iterable<T> ts) throws IOException {
    final Map<T, TimeSeries> map = new HashMap<T, TimeSeries>();
    for (final T t : ts) {
      final String id = id(t);
      map.put(t, this.database.createOrOpen(id, this.period, this.windows));
    }
    return map;
  }

  /**
   * Start the polling process.
   */
  @Idempotent
  public final void start() {
    for (final T t : BasePoller.this.ts) {
      //TODO Make sure this idempotent
      this.scheduler.schedule(new Runnable() {
        @Override
        public void run() {
          final long timestamp = System.currentTimeMillis();
          try {
            final Future<Integer> metric = metric(t);
            final Future<Void> wrapped = new FutureTask<Void>(new Callable<Void>() {
              @Override
              public Void call() throws Exception {
                throw new UnsupportedOperationException("Not supported yet.");
              }
            });
            BasePoller.this.timeseriess.get(t).publish(timestamp, metric.get());
          } catch (InterruptedException e) {
            //If metric extraction is too long process will be interrupted
            if (Loggers.BASE_LOGGER.isLoggable(Level.FINEST)) {
              Loggers.BASE_LOGGER.log(Level.FINEST, "Interrupted while publishing for <"+t+">", e);
            }
          } catch (Exception e) {
            if (Loggers.BASE_LOGGER.isLoggable(Level.WARNING)) {
              Loggers.BASE_LOGGER.log(Level.WARNING, "Got exception while accessing metric for <"+t+">", e);
            }
          }
        }
        @Override
        public String toString() {
          return "Poller";
        }
      });
    }
  }

  /**
   * @param t
   * @return a unique id for this {@link TimeSeries}
   */
  protected String id(final T t) {
    return t.toString();
  }

  /**
   * @param t
   * @return a {@link Future} holding the metric extracted for {@code t}
   * @throws Exception 
   */
  protected abstract Future<Integer> metric(T t) throws Exception;

  /**
   * @return a {@link Map} of all underlying {@link Reader}
   */
  public final Map<String, Map<Window, ? extends Reader>> getReaders() {
    final Builder<String, Map<Window, ? extends Reader>> builder = ImmutableMap.builder();
    for (final TimeSeries timeSeries : this.timeseriess.values()) {
      builder.put(timeSeries.getId(), timeSeries.getReaders());
    }
    return builder.build();
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