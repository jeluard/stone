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
import com.github.jeluard.stone.api.Window;
import com.github.jeluard.stone.consolidator.MaxConsolidator;
import com.github.jeluard.stone.consolidator.MinConsolidator;
import com.google.common.base.Optional;
import com.google.common.collect.Iterables;

import java.io.IOException;

import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.junit.Assert;
import org.junit.Test;

public abstract class BaseStorageTest<T extends Storage> {

  protected final Window createWindow() {
    return createWindow(10);
  }

  protected final Window createWindow(final int maxSize) {
    return Window.of(Duration.standardSeconds(1)).persistedDuring(Duration.standardSeconds(maxSize)).consolidatedBy(MaxConsolidator.class, MinConsolidator.class);
  }

  protected final T createStorage() throws IOException {
    return createStorage(createWindow());
  }

  protected final T createStorage(final int maxSize) throws IOException {
    return createStorage(createWindow(maxSize));
  }

  protected abstract T createStorage(Window window) throws IOException;

  @Test
  public void shouldEndBeAfterBeginningWhenDefined() throws IOException {
    final T storage = createStorage();
    final Optional<DateTime> beginning = storage.beginning();
    final Optional<DateTime> end = storage.end();
    if (beginning.isPresent() && end.isPresent()) {
      Assert.assertTrue(!beginning.get().isBefore(end.get()));
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
    storage.onConsolidation(timestamp, new int[]{1, 2});

    Assert.assertEquals(timestamp, storage.end().get().getMillis());
    Assert.assertEquals(timestamp, (long) Iterables.getLast(storage.all()).first);
  }

  @Test
  public void shouldNotGrowMoreThanMaxSize() throws Exception {
    final int maxSize = 10;
    final T storage = createStorage(maxSize);
    for (int i = 0; i < maxSize; i++) {
      storage.onConsolidation(i+1, new int[]{1, 2});
    }

    Assert.assertEquals(10, Iterables.size(storage.all()));
  }

  @Test
  public void shouldNotServeValueOlderThanDuration() throws Exception {
    final int maxSize = 10;
    final T storage = createStorage(maxSize);
    final DateTime now = DateTime.now();
    storage.onConsolidation(now.getMillis(), new int[]{1, 2});
    storage.onConsolidation(now.plus(Duration.standardMinutes(1)).plus(1).getMillis(), new int[]{1, 2});

    Assert.assertEquals(1, Iterables.size(storage.all()));
  }

  @Test
  public void shouldValuesBeInOrderAfterACycle() throws Exception {
    final int maxSize = 10;
    final T storage = createStorage(maxSize);
    for (int i = 0; i < 3*maxSize; i++) {
      storage.onConsolidation(i+1, new int[]{1, 2});
    }

    final long first = storage.beginning().get().getMillis();
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
      storage.onConsolidation(i+1, new int[]{1, 2});
    }

    Assert.assertEquals(3*maxSize, (long) Iterables.getLast(storage.all()).first);
  }

}