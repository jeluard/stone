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
package com.github.jeluard.stone.impl;

import com.github.jeluard.guayaba.base.Triple;
import com.github.jeluard.stone.api.ConsolidationListener;
import com.github.jeluard.stone.api.Consolidator;
import com.github.jeluard.stone.api.Window;
import com.github.jeluard.stone.helper.Loggers;
import com.github.jeluard.stone.spi.Storage;
import com.github.jeluard.stone.spi.StorageFactory;
import com.google.common.base.Preconditions;

import java.io.Closeable;
import java.io.IOException;
import java.util.logging.Level;

/**
 * Encapsulate the logic that triggers call to {@link Consolidator#accumulate(long, int)}, {@link Consolidator#consolidateAndReset()} and {@link Storage#append(long, int[])}.
 * <br>
 * A single {@link Engine} is available per {@link com.github.jeluard.stone.api.DataBase} thus shared among all associated {@link com.github.jeluard.stone.api.TimeSeries}.
 */
public final class Engine {

  private final StorageFactory<?> storageFactory;

  public Engine(final StorageFactory storageFactory) throws IOException {
    this.storageFactory = Preconditions.checkNotNull(storageFactory, "null storageFactory");
  }

  public StorageFactory<?> getStorageFactory() {
    return this.storageFactory;
  }

  /**
   * @param beginning
   * @param timestamp
   * @param duration
   * @return id of the {@link Window} containing {@code timestamp}
   */
  private long windowId(final long beginning, final long timestamp, final long duration) {
    return (timestamp - beginning) / duration;
  }

  /**
   * Propagates {@code timestamp}/{@code value} to all {@code consolidators} {@link Consolidator#accumulate(long, int)}.
   *
   * @param consolidators
   * @param timestamp
   * @param value 
   */
  private void accumulate(final Consolidator[] consolidators, final long timestamp, final int value) {
    for (final Consolidator consolidator : consolidators) {
      consolidator.accumulate(timestamp, value);
    }
  }

  /**
   * Generate all consolidates then propagates them to {@link Storage#append(long, int[])}.
   * <br>
   * If any, {@link ConsolidationListener#onConsolidation(com.github.jeluard.stone.api.Window, long, int[])} are then called.
   * Failure of one of {@link ConsolidationListener#onConsolidation(com.github.jeluard.stone.api.Window, long, int[])} does not prevent others to be called.
   *
   * @param timestamp
   * @param consolidators
   * @param storage
   * @param consolidationListeners
   * @param window passed to {@link ConsolidationListener#onConsolidation(com.github.jeluard.stone.api.Window, long, int[])}
   * @throws IOException 
   */
  private void persist(final long timestamp, final Consolidator[] consolidators, final Storage storage, final ConsolidationListener[] consolidationListeners, final Window window) throws IOException {
    //TODO Do not create arrays each time?
    final int[] consolidates = new int[consolidators.length];
    for (int i = 0; i < consolidators.length; i++) {
      consolidates[i] = consolidators[i].consolidateAndReset();
    }
    storage.append(timestamp, consolidates);

    for (final ConsolidationListener consolidationListener : consolidationListeners) {
      try {
        consolidationListener.onConsolidation(window, timestamp, consolidates);
      } catch (Exception e) {
        if (Loggers.BASE_LOGGER.isLoggable(Level.WARNING)) {
          Loggers.BASE_LOGGER.log(Level.WARNING, "Got exception while executing <"+consolidationListener+">", e);
        }
      }
    }
  }

  /**
   * Perform {@link Consolidator#accumulate(long, int)} and {@link Consolidator#consolidateAndReset()} for all {@link Window} depending on {@code timestamp}.
   * <br>
   * Result of {@link Consolidator#consolidateAndReset()} is then persisted (via {@link Storage#append(long, int[])}) through the right {@link Storage}.
   * <br>
   * <br>
   * When provided {@link ConsolidationListener}s are called <strong>after</strong> a successful {@link Storage#append(long, int[])}.
   *
   * @param triples
   * @param consolidationListeners
   * @param beginningTimestamp
   * @param previousTimestamp
   * @param currentTimestamp
   * @param value
   * @throws IOException 
   */
  public void publish(final Triple<Window, Storage, Consolidator[]>[] triples, final ConsolidationListener[] consolidationListeners, final long beginningTimestamp, final long previousTimestamp, final long currentTimestamp, final int value) throws IOException {
    for (final Triple<Window, Storage, Consolidator[]> triple : triples) {
      accumulate(triple.third, currentTimestamp, value);

      final long duration = triple.first.getResolution().getMillis();
      final long currentWindowId = windowId(beginningTimestamp, currentTimestamp, duration);
      final long previousWindowId = windowId(beginningTimestamp, previousTimestamp, duration);
      if (currentWindowId != previousWindowId) {
        final long previousWindowBeginning = beginningTimestamp + previousWindowId * duration;

        persist(previousWindowBeginning, triple.third, triple.second, consolidationListeners, triple.first);
      }
    }
  }

}