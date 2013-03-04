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
package com.github.jeluard.stone.spi;

import com.github.jeluard.guayaba.base.Pair;
import com.google.common.base.Optional;
import com.google.common.collect.Iterables;

import java.io.IOException;

import org.junit.Assert;
import org.junit.Test;

public abstract class BaseStorageTest<T extends Storage> {

  protected final T createEmptyStorage() throws IOException {
    return createStorage(0);
  }

  protected final T createStorage() throws IOException {
    return createStorage(10);
  }

  protected abstract T createStorage(int maximumSize) throws IOException;

  @Test
  public void shouldEndBeAfterBeginningWhenDefined() throws IOException {
    final T storage = createStorage();
    final Optional<Long> beginning = storage.beginning();
    final Optional<Long> end = storage.end();
    if (beginning.isPresent() && end.isPresent()) {
      Assert.assertTrue(beginning.get() < end.get());
    }
  }

  @Test
  public void shouldBeginningAndEndBeUndefinedWhenAllIsEmpty() throws IOException {
    final T storage = createStorage();
    final Iterable<?> iterable = storage.all();
    if (Iterables.isEmpty(iterable)) {
      Assert.assertFalse(storage.beginning().isPresent());
      Assert.assertFalse(storage.end().isPresent());
    }
  }

  @Test
  public void shouldBeginningAndEndReflectAll() throws IOException {
    final T storage = createStorage();
    final Iterable<?> all = storage.all();
    if (!Iterables.isEmpty(all)) {
      Assert.assertTrue(storage.beginning().isPresent());
      Assert.assertTrue(storage.end().isPresent());
    }
  }

  @Test
  public void shouldNewConsolidatesBeLatestFromAll() throws Exception {
    final T storage = createStorage();
    final long timestamp = 12345L;
    storage.append(timestamp, new int[]{1, 2});

    Assert.assertEquals(timestamp, (long) storage.end().get());
    Assert.assertEquals(timestamp, (long) Iterables.getLast(storage.all()).first);
  }

  @Test
  public void shouldNotGrowMoreThanMaxSize() throws Exception {
    final int maxSize = 10;
    final T storage = createStorage(maxSize);
    for (int i = 0; i < maxSize; i++) {
      storage.append(i+1, new int[]{1, 2});
    }

    Assert.assertEquals(10, Iterables.size(storage.all()));
  }

  @Test
  public void shouldEmptyStorageBeEmpty() throws IOException {
    final T storage = createEmptyStorage();
    Assert.assertTrue(Iterables.isEmpty(storage.all()));
  }

  @Test
  public void shouldValuesBeInOrderAfterACycle() throws Exception {
    final int maxSize = 10;
    final T storage = createStorage(maxSize);
    for (int i = 0; i < 3*maxSize; i++) {
      storage.append(i+1, new int[]{1, 2});
    }

    final long first = storage.beginning().get();
    long previous = first;
    for (final Pair<Long, int[]> pair : storage.all()) {
      if (pair.first < previous) {
        Assert.fail();
      }
      previous = pair.first;
    }
  }

  @Test
  public void shouldLastValueBeLastFromAllAfterACycle() throws Exception {
    final int maxSize = 10;
    final T storage = createStorage(maxSize);
    for (int i = 0; i < 3*maxSize; i++) {
      storage.append(i+1, new int[]{1, 2});
    }

    Assert.assertEquals(3*maxSize, (long) Iterables.getLast(storage.all()).first);
  }

}