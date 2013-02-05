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
package com.github.jeluard.stone.storage.memory;

import com.github.jeluard.guayaba.base.Pair;
import com.github.jeluard.stone.spi.Storage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;

/**
 * An in-memory {@link Storage} implementation.
 */
public final class MemoryStorage extends Storage {

  private final int maxSize;
  private final int nbConsolidators;
  private final AtomicLong index = new AtomicLong(0);
  private final AtomicLongArray timestamps;
  private final AtomicIntegerArray values;

  public MemoryStorage(final int maxSize, final int nbConsolidators) {
    this.maxSize = maxSize;
    this.nbConsolidators = nbConsolidators;
    this.timestamps = new AtomicLongArray(maxSize);
    this.values = new AtomicIntegerArray(maxSize*nbConsolidators);
  }

  @Override
  public Iterable<Pair<Long, int[]>> all() throws IOException {
    final List<Pair<Long, int[]>> all = new ArrayList<Pair<Long, int[]>>(this.maxSize);
    for (int i = 0; i < this.maxSize-1; i++) {
      final long timestamp = this.timestamps.get(i);
      if (timestamp == 0L) {
        break;
      }
      final int[] consolidates = new int[this.nbConsolidators];
      for (int j = 0; j < this.nbConsolidators; j++) {
        consolidates[j] = this.values.get(i*this.nbConsolidators+j);
      }
      all.add(new Pair<Long, int[]>(timestamp, consolidates));
    }
    return all;
  }

  private int index() {
    return (int) (this.index.getAndIncrement() % this.maxSize);
  }

  @Override
  public void onConsolidation(final long timestamp, final int[] consolidates) throws Exception {
    final int currentIndex = index();
    this.timestamps.set(currentIndex, timestamp);
    for (int i = 0; i < consolidates.length; i++) {
      this.values.set(currentIndex + i, consolidates[i]);
    }
  }

}