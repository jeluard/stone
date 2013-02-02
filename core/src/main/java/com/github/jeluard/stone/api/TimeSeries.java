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
import com.github.jeluard.guayaba.base.Triple;
import com.github.jeluard.guayaba.lang.Identifiable;
import com.github.jeluard.guayaba.lang.Iterables2;
import com.github.jeluard.stone.helper.Loggers;
import com.github.jeluard.stone.spi.Storage;
import com.github.jeluard.stone.spi.StorageFactory;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
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
  private final Collection<Archive> archives;
  private final Database database;
  private final Triple<Duration, Consolidator[], ConsolidationListener[]>[] flattened;
  private long beginning;
  private long latest;

  TimeSeries(final String id, final Duration granularity, final Collection<Archive> archives, final Database database) throws IOException {
    this.id = Preconditions.checkNotNull(id, "null id");
    this.granularity = (int) Preconditions.checkNotNull(granularity, "null granularity").getMillis();
    this.archives = Preconditions.checkNotNull(archives, "null archives");
    this.database = Preconditions.checkNotNull(database, "null database");
    final Collection<Triple<Duration, Consolidator[], ConsolidationListener[]>> flattenedList = createFlatten(database.storageFactory, id, archives);
    this.flattened = flattenedList.toArray(new Triple[flattenedList.size()]);

    final Interval initialSpan = extractInterval(Collections2.transform(Arrays.asList(this.flattened), new Function<Triple<Duration, Consolidator[], ConsolidationListener[]>, Pair<Duration, Storage>>() {
      @Override
      public Pair<Duration, Storage> apply(final Triple<Duration, Consolidator[], ConsolidationListener[]> input) {
        final Storage storage = extractStorage(input.third);
        return new Pair<Duration, Storage>(input.first, storage);
      }
    }));
    this.beginning = initialSpan.getStartMillis();
    this.latest = initialSpan.getEndMillis();
  }

  private Storage extractStorage(final ConsolidationListener[] consolidationListeners) {
    return (Storage) consolidationListeners[0];
  }

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
   * @return all {@link Consolidator} mapping to {@link Archive#getConsolidators()}
   * @see #createConsolidator(java.lang.Class)
   */
  private Consolidator[] createConsolidators(final Archive archive, final int maxSamples) {
    final Collection<Class<? extends Consolidator>> types = archive.getConsolidators();
    final Consolidator[] consolidators = new Consolidator[types.size()];
    for (final Iterables2.Indexed<Class<? extends Consolidator>> indexedType : Iterables2.withIndex(types)) {
      consolidators[indexedType.index] = createConsolidator(indexedType.value, maxSamples);
    }
    return consolidators;
  }

  /**
   * @param storageFactory
   * @param id
   * @param archive
   * @param window
   * @return the {@link Storage} instance for this {@link Window}
   * @throws IOException 
   */
  private Storage createStorage(final StorageFactory storageFactory, final String id, final Archive archive, final Window window) throws IOException {
    return storageFactory.createOrGet(id, archive, window);
  }

  /**
   * @param storageFactory
   * @param id
   * @param archives
   * @return all pair of {@link Window} and {@link Consolidator[]}. {@link Consolidator[]} is specific to associated {@link Window} 
   * @throws IOException 
   */
  private Collection<Triple<Duration, Consolidator[], ConsolidationListener[]>> createFlatten(final StorageFactory storageFactory, final String id, final Collection<Archive> archives) throws IOException {
    final Collection<Triple<Duration, Consolidator[], ConsolidationListener[]>> windowTriples = new LinkedList<Triple<Duration, Consolidator[], ConsolidationListener[]>>();
    for (final Archive archive : archives) {
      for (final Window window : archive.getWindows()) {
        final int maxSamples = (int) window.getResolution().getMillis() / this.granularity;
        final List<ConsolidationListener> consolidationListeners = new LinkedList<ConsolidationListener>();
        consolidationListeners.add(createStorage(storageFactory, id, archive, window));
        consolidationListeners.addAll(Arrays.asList(window.getConsolidationListeners()));
        windowTriples.add(new Triple<Duration, Consolidator[], ConsolidationListener[]>(window.getResolution(), createConsolidators(archive, maxSamples), consolidationListeners.toArray(new ConsolidationListener[consolidationListeners.size()])));
      }
    }
    return windowTriples;
  }

  /**
   * @param pairs
   * @return maximum {@link Interval} covered by all {@link Window}s
   * @throws IOException 
   */
  private Interval extractInterval(final Collection<Pair<Duration, Storage>> pairs) throws IOException {
    return new Interval(extractBeginning(pairs.iterator().next().second), extractLatest(pairs));
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
  private long extractLatest(final Collection<Pair<Duration, Storage>> pairs) throws IOException {
    long storageLatest = 0L;
    for (final Pair<Duration, Storage> pair : pairs) {
      final Storage storage = pair.second;
      final Optional<DateTime> optionalInterval = storage.end(); 
      if (optionalInterval.isPresent()) {
        final long endInterval = optionalInterval.get().getMillis();
        if ((storageLatest == 0L) || (endInterval > storageLatest)) {
          storageLatest = endInterval + pair.first.getMillis();
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
  public List<Reader> getReaders() {
    final List<Reader> readers = new LinkedList<Reader>();
    for (final Triple<Duration, Consolidator[], ConsolidationListener[]> triple : this.flattened) {
      readers.add(extractStorage(triple.third));
    }
    return readers;
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
      this.database.dispatcher.publish(this.flattened, this.beginning, previousTimestamp, timestamp, value);
    }
    return true;
  }

  @Idempotent
  public void close() {
    this.database.remove(this.id);
    for (final Archive archive : this.archives) {
      for (final Window window : archive.getWindows()) {
        try {
          this.database.storageFactory.close(this.id, archive, window);
        } catch (IOException e) {
          if (Loggers.BASE_LOGGER.isLoggable(Level.WARNING)) {
            Loggers.BASE_LOGGER.log(Level.WARNING, "Got exception while closing "+Storage.class.getSimpleName()+" for <"+this.id+"> <"+archive+"> <"+window+">", e);
          }
        }
      }
    }
  }

}