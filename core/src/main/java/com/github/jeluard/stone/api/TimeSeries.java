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
import com.github.jeluard.guayaba.base.Pair;
import com.github.jeluard.guayaba.base.Pairs;
import com.github.jeluard.guayaba.base.Triple;
import com.github.jeluard.guayaba.lang.Identifiable;
import com.github.jeluard.guayaba.lang.Iterables2;
import com.github.jeluard.stone.helper.Loggers;
import com.github.jeluard.stone.spi.Dispatcher;
import com.github.jeluard.stone.spi.Storage;
import com.github.jeluard.stone.spi.StorageFactory;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import javax.annotation.concurrent.NotThreadSafe;

import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.Interval;

/**
 * Main abstraction allowing to publish {@code timestamp}/{@code value} pair.
 * Each published value is passed to all associated {@link Consolidator} (defined in {@link Archive#getConsolidators()}) first for accumulation (see {@link Consolidator#accumulate(long, int)})
 * then each time a {@link Window#getResolution()} threshold is crossed for consolidation (see {@link Consolidator#consolidateAndReset()}).
 * Final consolidated results are persisted in a {@link TimeSeries}/{@link Window} specific {@link Storage}.
 * <br>
 * {@link TimeSeries} is not threadsafe and supposed to be manipulated from a unique thread.
 *
 * @see Database
 */
@NotThreadSafe
public final class TimeSeries implements Identifiable<String> {

  private static final int MAX_SAMPLES_WARNING_THRESHOLD = 1000 * 1000;

  private final String id;
  private final int granularity;
  private final Dispatcher dispatcher;
  private final StorageFactory<?> storageFactory;
  private final Triple<Window, Consolidator[], ConsolidationListener[]>[] flattened;
  private final Map<Window, ? extends Reader> readers;
  private long beginning = 0L;
  private long latest = 0L;

  TimeSeries(final String id, final Duration granularity, final Window[] windows, final Dispatcher dispatcher, final StorageFactory<?> storageFactory) throws IOException {
    this.id = Preconditions.checkNotNull(id, "null id");
    this.granularity = (int) Preconditions.checkNotNull(granularity, "null granularity").getMillis();
    this.storageFactory = Preconditions.checkNotNull(storageFactory, "null storageFactory");
    this.dispatcher = Preconditions.checkNotNull(dispatcher, "null dispatcher");
    final Collection<Triple<Duration, Consolidator[], ConsolidationListener[]>> flattenedList = createFlatten(storageFactory, id, Preconditions.checkNotNull(windows, "null windows"));
    this.flattened = flattenedList.toArray(new Triple[flattenedList.size()]);
    final Collection<Pair<Window, Storage>> storagePerWindow = filterTripleWithStorage(this.flattened);
    this.readers = Pairs.toMap(storagePerWindow);

    final Optional<Interval> optionalInitialSpan = extractInterval(storagePerWindow);
    if (optionalInitialSpan.isPresent()) {
      final Interval initialSpan = optionalInitialSpan.get();
      this.beginning = initialSpan.getStartMillis();
      this.latest = initialSpan.getEndMillis();
    }
  }

  private Collection<Pair<Window, Storage>> filterTripleWithStorage(final Triple<Window, Consolidator[], ConsolidationListener[]>[] triples) {
    final List<Pair<Window, Storage>> pairs = new LinkedList<Pair<Window, Storage>>();
    for (final Triple<Window, Consolidator[], ConsolidationListener[]> triple : triples) {
      if (triple.third[0] instanceof Storage) {
        pairs.add(new Pair<Window, Storage>(triple.first, (Storage) triple.third[0]));
      }
    }
    return pairs;
  }

  /*private Optional<Storage> extractStorage(final ConsolidationListener[] consolidationListeners) {
    for (final ConsolidationListener consolidationListener : consolidationListeners) {
      
    }
    return (Storage) consolidationListeners[0];
  }*/

  /**
   * Instantiate a {@link Consolidator} from specified {@code type}.
   * First look for a constructor accepting an int as unique argument then fallback to the default constructor.
   *
   * @param type
   * @param maxSamples
   * @return a {@link Consolidator} from specified {@code type}
   */
  private Consolidator createConsolidator(final Class<? extends Consolidator> type, final int maxSamples) {
    try {
      //First look for a constructor accepting int as argument
      try {
        final Constructor<? extends Consolidator> samplesConstructor = type.getConstructor(int.class);
        if (maxSamples > TimeSeries.MAX_SAMPLES_WARNING_THRESHOLD) {
          if (Loggers.BASE_LOGGER.isLoggable(Level.WARNING)) {
            Loggers.BASE_LOGGER.log(Level.WARNING, "Using <{0}> as maxSamples for <{1}>; this might lead to excessive memory consumption.", new Object[]{maxSamples, type});
          }
        }
        return samplesConstructor.newInstance(maxSamples);
      } catch (NoSuchMethodException e) {
        if (Loggers.BASE_LOGGER.isLoggable(Level.FINEST)) {
          Loggers.BASE_LOGGER.log(Level.FINEST, "{0} does not define a constructor accepting int", type.getCanonicalName());
        }
      }

      //If we can't find such fallback to defaut constructor
      try {
        final Constructor<? extends Consolidator> defaultConstructor = type.getConstructor();
        return defaultConstructor.newInstance();
      } catch (NoSuchMethodException e) {
        throw new IllegalArgumentException("Failed to find int or default constructor for "+type.getCanonicalName(), e);
      }
    } catch (ReflectiveOperationException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * @param archive
   * @param maxSamples
   * @return all {@link Consolidator} mapping to {@link Window#getConsolidatorTypes()}
   * @see #createConsolidator(java.lang.Class)
   */
  private Consolidator[] createConsolidators(final Window window, final int maxSamples) {
    final List<? extends Class<? extends Consolidator>> consolidatorTypes = window.getConsolidatorTypes();
    final Consolidator[] consolidators = new Consolidator[consolidatorTypes.size()];
    for (final Iterables2.Indexed<? extends Class<? extends Consolidator>> indexed : Iterables2.withIndex(consolidatorTypes)) {
      consolidators[indexed.index] = createConsolidator(indexed.value, maxSamples);
    }
    return consolidators;
  }

  /**
   * @param storageFactory
   * @param id
   * @param window
   * @param duration
   * @return the {@link Storage} instance for this {@link Window}
   * @throws IOException 
   */
  private Storage createStorage(final StorageFactory storageFactory, final String id, final Window window, final Duration duration) throws IOException {
    return storageFactory.createOrGet(id, window, duration);
  }

  /**
   * @param storageFactory
   * @param id
   * @param windows
   * @return all pair of {@link Window} and {@link Consolidator[]}. {@link Consolidator[]} is specific to associated {@link Window} 
   * @throws IOException 
   */
  private Collection<Triple<Duration, Consolidator[], ConsolidationListener[]>> createFlatten(final StorageFactory storageFactory, final String id, final Window[] windows) throws IOException {
    final Collection<Triple<Duration, Consolidator[], ConsolidationListener[]>> windowTriples = new LinkedList<Triple<Duration, Consolidator[], ConsolidationListener[]>>();
    for (final Window window : windows) {
      final int maxSamples = (int) window.getResolution().getMillis() / this.granularity;
      final List<ConsolidationListener> consolidationListeners = new LinkedList<ConsolidationListener>();
      final Optional<Duration> optionalDuration = window.getPersistentDuration();
      if (optionalDuration.isPresent()) {
        consolidationListeners.add(createStorage(storageFactory, id, window, optionalDuration.get()));
      }
      consolidationListeners.addAll(window.getConsolidationListeners());
      windowTriples.add(new Triple<Duration, Consolidator[], ConsolidationListener[]>(window.getResolution(), createConsolidators(window, maxSamples), consolidationListeners.toArray(new ConsolidationListener[consolidationListeners.size()])));
    }
    return windowTriples;
  }

  /**
   * @param pairs
   * @return maximum {@link Interval} covered by all {@link Window}s
   * @throws IOException 
   */
  private Optional<Interval> extractInterval(final Collection<Pair<Window, Storage>> pairs) throws IOException {
    if (pairs.isEmpty()) {
      return Optional.absent();
    }
    return Optional.of(new Interval(extractBeginning(pairs.iterator().next().second), extractLatest(pairs)));
  }

  /**
   * @param storage
   * @return beginning of {@link Storage#beginning()} if any; 0L otherwise
   * @throws IOException 
   */
  private long extractBeginning(final Storage storage) throws IOException {
    final Optional<DateTime> interval = storage.beginning();
    if (interval.isPresent()) {
      return interval.get().getMillis();
    }
    return 0L;
  }

  /**
   * @param pairs
   * @return latest (more recent) timestamp stored in specified {@code storages}; 0L if all {@code storages} are empty
   * @throws IOException 
   */
  private long extractLatest(final Collection<Pair<Window, Storage>> pairs) throws IOException {
    long storageLatest = 0L;
    for (final Pair<Window, Storage> pair : pairs) {
      final Storage storage = pair.second;
      final Optional<DateTime> optionalInterval = storage.end(); 
      if (optionalInterval.isPresent()) {
        final long endInterval = optionalInterval.get().getMillis();
        if ((storageLatest == 0L) || (endInterval > storageLatest)) {
          storageLatest = endInterval + pair.first.getResolution().getMillis();
        }
      }
    }
    return storageLatest;
  }

  /**
   * @return a unique id identifying this {@link TimeSeries} in associated {@link Database}
   */
  @Override
  public String getId() {
    return this.id;
  }

  /**
   * @return all underlying {@link Reader} ordered by {@link Window}
   */
  public Map<Window, ? extends Reader> getReaders() {
    return this.readers;
  }

  /**
   * @param previousTimestamp
   * @param currentTimestamp
   * @return true if timestamp is after previous one (considering granularity)
   */
  private boolean isAfterPreviousTimestamp(final long previousTimestamp, final long currentTimestamp, final long granularity) {
    return (currentTimestamp - previousTimestamp) >= granularity;
  }

  /**
   * @param timestamp
   * @return latest {@code timestamp} published
   */
  private long recordTimestamp(final long timestamp) {
    final long previousTimestamp = this.latest;
    this.latest = timestamp;
    //If beginning is still default (i.e. all storages where empty) sets it's value to timestamp
    //This will be done only once (hence track the first timestamp received)
    if (this.beginning == 0L) {
      this.beginning = timestamp;
    }
    return previousTimestamp;
  }

  /**
   * Publish a {@code timestamp}/{@code value} pair triggering accumulation/persistency process.
   * <br>
   * {@code timestamp}s must be monotonic (i.e. each timestamp must be strictly greate than the previous one).
   * <br>
   * {@code timestamp}s are assumed to be in milliseconds. No timezone information is considered so {@code timestamp} values must be normalized before being published.
   *
   * @param timestamp
   * @param value
   * @return true if {@code value} has been considered for accumulation (i.e. it is not a value in the past)
   */
  public boolean publish(final long timestamp, final int value) {
    final long previousTimestamp = recordTimestamp(timestamp);
    final boolean isAfter = isAfterPreviousTimestamp(previousTimestamp, timestamp, this.granularity);
    if (!isAfter) {
      //timestamp is older that what can be accepted; return early
      return false;
    }

    //previousTimestamp == 0 if this is the first publish call and associated storage was empty (or new)
    if (previousTimestamp != 0L) {
      this.dispatcher.publish(this.flattened, this.beginning, previousTimestamp, timestamp, value);
    }
    return true;
  }

  @Idempotent
  public void close() {
    for (final Triple<Window, ?, ?> triple : this.flattened) {
      final Window window = triple.first;
      try {
        this.storageFactory.close(this.id, window);
      } catch (IOException e) {
        if (Loggers.BASE_LOGGER.isLoggable(Level.WARNING)) {
          Loggers.BASE_LOGGER.log(Level.WARNING, "Got exception while closing "+Storage.class.getSimpleName()+" for <"+this.id+"> <"+window+">", e);
        }
      }
    }
  }

}