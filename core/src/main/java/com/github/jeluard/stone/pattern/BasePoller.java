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
import com.github.jeluard.guayaba.base.Triple;
import com.github.jeluard.guayaba.lang.Cancelable;
import com.github.jeluard.guayaba.util.concurrent.Scheduler;
import com.github.jeluard.stone.api.Database;
import com.github.jeluard.stone.api.Reader;
import com.github.jeluard.stone.api.TimeSeries;
import com.github.jeluard.stone.api.Window;
import com.github.jeluard.stone.helper.Loggers;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import com.google.common.util.concurrent.Uninterruptibles;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.joda.time.Duration;

/**
 * Base implementation tracking a metric for a collection of resources <T> periodically.
 */
public abstract class BasePoller<T> implements Cancelable {

  private static final Logger LOGGER = Loggers.create("poller");

  private final Database database;
  private final Duration period;
  private final Collection<T> ts;
  private final Window[] windows;
  private final Map<T, TimeSeries> timeseriess;
  private final Scheduler scheduler;

  public BasePoller(final Database database, final Collection<T> ts, final Duration period, final Collection<Window> windows, final ExecutorService schedulerExecutorService) throws IOException {
    this.database = Preconditions.checkNotNull(database, "null database");
    this.period = Preconditions.checkNotNull(period, "null period");
    this.ts = Preconditions.checkNotNull(ts, "null ts");
    this.windows = Preconditions.checkNotNull(windows, "null windows").toArray(new Window[windows.size()]);
    this.timeseriess = createTimeSeries(ts);
    this.scheduler = new Scheduler(period.getMillis(), TimeUnit.MILLISECONDS, schedulerExecutorService, Loggers.BASE_LOGGER);
  }

  public BasePoller(final Database database, final Collection<T> ts, final Duration period, final Collection<Window> windows) throws IOException {
    this(database, ts, period, windows, Scheduler.defaultExecutorService(10, Loggers.BASE_LOGGER));
  }

  private Map<T, TimeSeries> createTimeSeries(final Iterable<T> ts) throws IOException {
    final Map<T, TimeSeries> map = new HashMap<T, TimeSeries>();
    for (final T t : ts) {
      final String id = id(t);
      map.put(t, this.database.createOrOpen(id, this.period, this.windows));
    }
    return map;
  }

  protected long initialWaitTime() {
    return this.period.getMillis() / 10;
  }

  protected long perCheckWaitTime() {
    return 1;
  }

  /**
   * Start the polling process.
   */
  @Idempotent
  public final void start() {
    this.scheduler.schedule(new Runnable() {
      @Override
      public void run() {
        final List<Triple<T, Long, Future<Integer>>> futures = new ArrayList<Triple<T, Long, Future<Integer>>>(BasePoller.this.ts.size());
        for (final T t : BasePoller.this.ts) {
          futures.add(new Triple<T, Long, Future<Integer>>(t, System.currentTimeMillis(), metric(t)));
        }

        //Do not directly check all results but wait to limit useless checks
        final long initialWaitTime = initialWaitTime();
        if (initialWaitTime > 0) {
          try {
            Thread.sleep(initialWaitTime);
          } catch (InterruptedException e) {
            return;
          }
        }

        while (!futures.isEmpty()) {
          if (Thread.currentThread().isInterrupted()) {
            cancelRemaining(futures);
            break;
          }

          for (final Iterator<Triple<T, Long, Future<Integer>>> it = futures.iterator(); it.hasNext();) {
            final Triple<T, Long, Future<Integer>> triple = it.next();
            final long timestamp = triple.second;
            final Future<Integer> future = triple.third;
            if (future.isDone()) {
              try {
                it.remove();
                publish(triple.first, timestamp, Uninterruptibles.getUninterruptibly(future));
              } catch (ExecutionException e) {
                if (BasePoller.LOGGER.isLoggable(Level.WARNING)) {
                  BasePoller.LOGGER.log(Level.WARNING, "Got exception while executing <"+triple.second+">", e);
                }
              }
            }
          }

          final long perCheckWaitTime = perCheckWaitTime();
          if (perCheckWaitTime > 0) {
            try {
              Thread.sleep(perCheckWaitTime);
            } catch (InterruptedException e) {
              break;
            }
          }
        }
      }
      @Override
      public String toString() {
        return "Poller";
      }

      private void cancelRemaining(final List<Triple<T, Long, Future<Integer>>> futures) {
        for (final Triple<T, Long, Future<Integer>> triple : futures) {
          triple.third.cancel(true);
        }
      }
    });
  }

  private void publish(final T timeSeries, final long timestamp, final Integer value) {
    this.timeseriess.get(timeSeries).publish(timestamp, value);
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
   */
  protected abstract Future<Integer> metric(T t);

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
  }

}