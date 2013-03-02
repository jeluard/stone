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

  private final AtomicLong index = new AtomicLong(0);
  private final int nbValues;
  private final AtomicLongArray timestamps;
  private final AtomicIntegerArray values;

  public MemoryStorage(final int maximumSize) {
    super(maximumSize);

    this.nbValues = 1;//TODO
    this.timestamps = new AtomicLongArray(getMaximumSize());
    this.values = new AtomicIntegerArray(getMaximumSize()*nbValues);
  }

  @Override
  public Iterable<Pair<Long, int[]>> all() throws IOException {
    final int maxSize = getMaximumSize();
    final List<Pair<Long, int[]>> all = new ArrayList<Pair<Long, int[]>>(maxSize);
    for (int i = 0; i < maxSize; i++) {
      final long timestamp = this.timestamps.get(i);
      if (timestamp == 0L) {
        break;
      }
      final int[] consolidates = new int[this.nbValues];
      for (int j = 0; j < this.nbValues; j++) {
        consolidates[j] = this.values.get(i*this.nbValues+j);
      }
      all.add(new Pair<Long, int[]>(timestamp, consolidates));
    }
    return all;
  }

  private int index() {
    return (int) (this.index.getAndIncrement() % getMaximumSize());
  }

  @Override
  public void append(final long timestamp, final int[] values) {
    final int currentIndex = index();
    this.timestamps.set(currentIndex, timestamp);
    for (int i = 0; i < values.length; i++) {
      this.values.set(currentIndex + i, values[i]);
    }
  }

}