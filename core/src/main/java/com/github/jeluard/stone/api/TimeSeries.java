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

import com.github.jeluard.guayaba.base.Pair;
import com.github.jeluard.guayaba.base.Triple;
import com.github.jeluard.guayaba.lang.Iterables2;
import com.github.jeluard.stone.helper.Loggers;
import com.github.jeluard.stone.impl.Engine;
import com.github.jeluard.stone.spi.Storage;
import com.github.jeluard.stone.spi.StorageFactory;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.Collections2;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.logging.Level;

import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.Interval;

/**
 * Main abstraction allowing to publish {@code timestamp}/{@code value} pair.
 * Each published value is passed to all associated {@link Consolidator} (defined in {@link Archive#getConsolidators()}) first for accumulation (see {@link Consolidator#accumulate(long, int)})
 * then each time a {@link Window#getResolution()} threshold is crossed for consolidation (see {@link Consolidator#consolidateAndReset()}).
 * Final consolidated results are persisted in a {@link TimeSeries}/{@link Window} specific {@link Storage}.
 *
 * @see Database
 */
public final class TimeSeries {

  private final String id;
  private final int granularity;
  private final ConsolidationListener[] consolidationListeners;
  private final Engine engine;
  private final Triple<Window, Storage, Consolidator[]>[] flattened;
  private long beginning;
  private long latest;

  TimeSeries(final String id, final Duration granularity, final Collection<Archive> archives, final Collection<ConsolidationListener> consolidationListeners, final Engine engine) throws IOException {
    this.id = Preconditions.checkNotNull(id, "null id");
    this.granularity = (int) Preconditions.checkNotNull(granularity, "null granularity").getMillis();
    this.consolidationListeners = Preconditions.checkNotNull(consolidationListeners, "null consolidationListeners").toArray(new ConsolidationListener[consolidationListeners.size()]);
    this.engine = Preconditions.checkNotNull(engine, "null engine");
    final Collection<Triple<Window, Storage, Consolidator[]>> flattenedList = createFlatten(engine.getStorageFactory(), id, archives);
    this.flattened = flattenedList.toArray(new Triple[flattenedList.size()]);

    final Interval initialSpan = extractInterval(Collections2.transform(Arrays.asList(this.flattened), new Function<Triple<Window, Storage, Consolidator[]>, Pair<Window, Storage>>() {
      @Override
      public Pair<Window, Storage> apply(final Triple<Window, Storage, Consolidator[]> input) {
        return new Pair<Window, Storage>(input.first, input.second);
      }
    }));
    this.beginning = initialSpan.getStartMillis();
    this.latest = initialSpan.getEndMillis();
  }

  /**
   * Instantiate a {@link Consolidator} from specified {@code type}.
   * First look for a constructor accepting an int as unique argument then fallback to the default constructor.
   *
   * @param type
   * @param window
   * @return a {@link Consolidator} from specified {@code type}
   */
  private Consolidator createConsolidator(final Class<? extends Consolidator> type, final Window window) {
    try {
      //First look for a constructor accepting int as argument
      try {
        final Constructor<? extends Consolidator> samplesConstructor = type.getConstructor(int.class);
        final int maxSamples = (int) window.getDuration().getMillis() / this.granularity;
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
        throw new IllegalArgumentException("Failed to find int or default constructor for "+type.getCanonicalName());
      }
    } catch (ReflectiveOperationException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * @param archive
   * @param window
   * @return all {@link Consolidator} mapping to {@link Archive#getConsolidators()}
   * @see #createConsolidator(java.lang.Class)
   */
  private Consolidator[] createConsolidators(final Archive archive, final Window window) {
    final Collection<Class<? extends Consolidator>> types = archive.getConsolidators();
    final Consolidator[] consolidators = new Consolidator[types.size()];
    for (final Iterables2.Indexed<Class<? extends Consolidator>> indexedType : Iterables2.withIndex(types)) {
      consolidators[indexedType.index] = createConsolidator(indexedType.value, window);
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
   * @return all triplet of {@link Window}, {@link Storage} and {@link Consolidator[]}. Both {@link Storage} and {@link Consolidator[]} are specific to associated {@link Window} 
   * @throws IOException 
   */
  private Collection<Triple<Window, Storage, Consolidator[]>> createFlatten(final StorageFactory storageFactory, final String id, final Collection<Archive> archives) throws IOException {
    final Collection<Triple<Window, Storage, Consolidator[]>> windowTriples = new LinkedList<Triple<Window, Storage, Consolidator[]>>();
    for (final Archive archive : archives) {
      for (final Window window : archive.getWindows()) {
        windowTriples.add(new Triple<Window, Storage, Consolidator[]>(window, createStorage(storageFactory, id, archive, window), createConsolidators(archive, window)));
      }
    }
    return windowTriples;
  }

  /**
   * @param pairs
   * @return maximum {@link Interval} covered by all {@link Window}s
   * @throws IOException 
   */
  private Interval extractInterval(final Collection<Pair<Window, Storage>> pairs) throws IOException {
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
  public String getId() {
    return this.id;
  }

  /**
   * @return all underlying {@link Reader} mapped by {@link Window}
   */
  public Map<Window, Reader> getReaders() {
    final Map<Window, Reader> readers = new HashMap<Window, Reader>();
    for (final Triple<Window, Storage, Consolidator[]> triple : this.flattened) {
      readers.put(triple.first, triple.second);
    }
    return readers;
  }

  private void checkNotBeforeLatestTimestamp(final long previousTimestamp, final long currentTimestamp) {
    if (!((currentTimestamp - previousTimestamp) >= this.granularity)) {
      throw new IllegalArgumentException("Provided timestamp <"+currentTimestamp+"> must be greater than <"+(previousTimestamp + this.granularity)+"> (granularity <"+this.granularity+"> ms)");
    }
  }

  /**
   * @param timestamp
   * @return latest {@code timestamp} published
   */
  private long recordLatest(final long timestamp) {
    //Set to the new timestamp if value is null (i.e. no value as yet been recorded)
    final long previousTimestamp = this.latest;
    this.latest = timestamp;
    checkNotBeforeLatestTimestamp(previousTimestamp, timestamp);
    return previousTimestamp;
  }

  /**
   * @param timestamp
   * @return beginning {@code timestamp} for this {@link TimeSeries}. Either last value stored in {@link Storage} if any or first published in this run.
   */
  private long inferBeginning(final long timestamp) {
    //If beginning is still null (i.e. all storages where empty) sets it's value to timestamp
    //This will be done only once (hence track the first timestamp received)
    if (this.beginning == 0L) {
      this.beginning = timestamp;
    }
    return this.beginning;
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
   * @throws IOException 
   */
  public void publish(final long timestamp, final int value) throws IOException {
    Preconditions.checkNotNull(timestamp, "null timestamp");

    final long previousTimestamp = recordLatest(timestamp);
    final long beginningTimestamp = inferBeginning(timestamp);

    //previousTimestamp == 0 if this is the first publish call and associated storage was empty (or new)
    if (previousTimestamp != 0L) {
      this.engine.publish(this.flattened, this.consolidationListeners, beginningTimestamp, previousTimestamp, timestamp, value);
    }
  }

}