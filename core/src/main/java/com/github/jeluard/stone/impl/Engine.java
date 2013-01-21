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
import com.github.jeluard.stone.spi.Storage;
import com.github.jeluard.stone.spi.StorageFactory;
import com.google.common.base.Preconditions;

import java.io.Closeable;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 */
public final class Engine {

  private static final Logger LOGGER = Logger.getLogger("com.github.jeluard.stone");

  private final StorageFactory<?> storageFactory;

  public Engine(final StorageFactory storageFactory) throws IOException {
    this.storageFactory = Preconditions.checkNotNull(storageFactory, "null storageFactory");
  }

  public StorageFactory<?> getStorageFactory() {
    return this.storageFactory;
  }

  private long windowId(final long beginning, final long timestamp, final long duration) {
    return (timestamp - beginning) / duration;
  }

  private void accumulate(final Consolidator[] consolidators, final long timestamp, final int value) {
    for (final Consolidator consolidator : consolidators) {
      consolidator.accumulate(timestamp, value);
    }
  }

  private void persist(final Window window, final Consolidator[] consolidators, final long timestamp, final Storage storage, final ConsolidationListener[] consolidationListeners) throws IOException {
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
        if (Engine.LOGGER.isLoggable(Level.WARNING)) {
          Engine.LOGGER.log(Level.WARNING, "Got exception while executing <"+consolidationListener+">", e);
        }
      }
    }
  }

  public void publish(final Triple<Window, Storage, Consolidator[]>[] triples, final ConsolidationListener[] consolidationListeners, final long beginningTimestamp, final long previousTimestamp, final long currentTimestamp, final int value) throws IOException {
    for (final Triple<Window, Storage, Consolidator[]> triple : triples) {
      accumulate(triple.third, currentTimestamp, value);

      //previousTimestamp == 0 if this is the first publish call and associated storage was empty (or new)
      if (previousTimestamp != 0L) {
        final long duration = triple.first.getResolution().getMillis();
        final long currentWindowId = windowId(beginningTimestamp, currentTimestamp, duration);
        final long previousWindowId = windowId(beginningTimestamp, previousTimestamp, duration);
        if (currentWindowId != previousWindowId) {
          final long previousWindowBeginning = beginningTimestamp + previousWindowId * duration;

          persist(triple.first, triple.third, previousWindowBeginning, triple.second, consolidationListeners);
        }
      }
    }
  }

}