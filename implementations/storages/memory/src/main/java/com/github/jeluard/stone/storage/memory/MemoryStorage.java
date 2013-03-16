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
import com.google.common.primitives.Longs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.atomic.AtomicReferenceArray;

/**
 * An in-memory {@link Storage} implementation.
 */
public final class MemoryStorage extends Storage {

  private final AtomicLong index = new AtomicLong(0);
  private final AtomicLongArray timestamps;
  private final AtomicReferenceArray<int[]> values;

  public MemoryStorage(final int maximumSize) {
    super(maximumSize);

    this.timestamps = new AtomicLongArray(getMaximumSize());
    this.values = new AtomicReferenceArray<int[]>(getMaximumSize());
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

      all.add(new Pair<Long, int[]>(timestamp, this.values.get(i)));
    }
    Collections.sort(all, new Comparator<Pair<Long, int[]>>() {
      @Override
      public int compare(final Pair<Long, int[]> pair1, Pair<Long, int[]> pair2) {
        return Longs.compare(pair1.first, pair2.first);
      }
    });
    return all;
  }

  private int index() {
    return (int) (this.index.getAndIncrement() % getMaximumSize());
  }

  @Override
  public void append(final long timestamp, final int[] values) {
    final int currentIndex = index();
    this.timestamps.set(currentIndex, timestamp);
    this.values.set(currentIndex, values.clone());
  }

}