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
import com.github.jeluard.stone.api.Reader;
import com.github.jeluard.stone.api.TimeSeries;
import com.github.jeluard.stone.api.Window;
import com.github.jeluard.stone.helper.Loggers;
import com.github.jeluard.stone.spi.Dispatcher;
import com.github.jeluard.stone.spi.StorageFactory;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import com.google.common.util.concurrent.Uninterruptibles;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Base implementation tracking a metric for a collection of resources <T> periodically.
 */
public class Poller<T> implements Cancelable {

  private static final Logger LOGGER = Loggers.create("poller");

  private final Database database;
  private final Long period;
  private final Window[] windows;
  private final Function<T, String> idExtractor;
  private final Function<T, Future<Integer>> metricExtractor;
  private final ConcurrentMap<T, TimeSeries> timeseriess = new ConcurrentHashMap<T, TimeSeries>();
  private final Scheduler scheduler;

  public Poller(final long period, final Collection<Window> windows, final Function<T, String> idExtractor, final Function<T, Future<Integer>> metricExtractor, final Dispatcher dispatcher, final StorageFactory<?> storageFactory, final ExecutorService schedulerExecutorService) {
    this.period = Preconditions.checkNotNull(period, "null period");
    this.windows = Preconditions.checkNotNull(windows, "null windows").toArray(new Window[windows.size()]);
    this.idExtractor = Preconditions.checkNotNull(idExtractor, "null idExtractor");
    this.metricExtractor = Preconditions.checkNotNull(metricExtractor, "null metricExtractor");
    this.database = new Database(Preconditions.checkNotNull(dispatcher, "null dispatcher"), Preconditions.checkNotNull(storageFactory, "null storageFactory"));
    this.scheduler = new Scheduler(period, TimeUnit.MILLISECONDS, schedulerExecutorService, Loggers.BASE_LOGGER);
  }

  public static <U> Function<U, String> defaultIdExtractor() {
    return new Function<U, String>() {
      @Override
      public String apply(final U input) {
        return input.toString();
      }
    };
  }

  private TimeSeries createTimeSeries(final T t) throws IOException {
    final String id = this.idExtractor.apply(t);
    return this.database.createOrOpen(id, this.period, this.windows);
  }

  protected long initialWaitTime() {
    return this.period / 10;
  }

  protected long perCheckWaitTime() {
    return 1;
  }

  public final void enqueue(final T t) throws IOException {
    Preconditions.checkNotNull(t, "null t");

    this.timeseriess.put(t, createTimeSeries(t));
  }

  public final boolean dequeue(final T t) {
    //TODO queue removal so that ts are not removed during a publication cycle?
    Preconditions.checkNotNull(t, "null t");

    return this.timeseriess.remove(t) != null;
  }

  /**
   * Start the polling process.
   */
  @Idempotent
  public final void start() {
    this.scheduler.schedule(new Runnable() {
      @Override
      public void run() {
        final List<Triple<T, Long, Future<Integer>>> futures = new ArrayList<Triple<T, Long, Future<Integer>>>(Poller.this.timeseriess.size());
        for (final T t : Poller.this.timeseriess.keySet()) {
          futures.add(new Triple<T, Long, Future<Integer>>(t, System.currentTimeMillis(), Poller.this.metricExtractor.apply(t)));
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
                if (Poller.LOGGER.isLoggable(Level.WARNING)) {
                  Poller.LOGGER.log(Level.WARNING, "Got exception while executing <"+triple.second+">", e);
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

  private void publish(final T t, final long timestamp, final Integer value) {
    final TimeSeries timeSeries = this.timeseriess.get(t);
    if (timeSeries == null) {
      //t has been removed during a publication cycle
      if (Loggers.BASE_LOGGER.isLoggable(Level.FINE)) {
        Loggers.BASE_LOGGER.fine("Failed to access time series for <"+t+">");
      }

      return;
    }

    timeSeries.publish(timestamp, value);
  }

  /**
   * @return a {@link Map} of all underlying {@link Reader}
   */
  public final Map<String, Map<Window, ? extends Reader>> getReaders() {
    final Builder<String, Map<Window, ? extends Reader>> builder = ImmutableMap.builder();
    for (final TimeSeries timeSeries : this.timeseriess.values()) {
      final String id = timeSeries.getId();
      final Optional<Map<Window, ? extends Reader>> optional = this.database.getReaders(id);
      if (optional.isPresent()) {
         builder.put(id, optional.get());
      }
    }
    return builder.build();
  }

  @Override
  public final void cancel() {
    this.scheduler.cancel();
    this.database.close();
  }

}