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

import com.google.common.base.Optional;
import com.google.common.collect.Iterables;

import java.io.IOException;

import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Test;

public abstract class BaseStorageTest<T extends Storage> {

  protected abstract T createStorage() throws IOException;

  @Test
  public void shouldEndBeAfterBeginningWhenDefined() throws IOException {
    final T storage = createStorage();
    final Optional<DateTime> beginning = storage.beginning();
    final Optional<DateTime> end = storage.end();
    if (beginning.isPresent() && end.isPresent()) {
      Assert.assertTrue(end.get().isAfter(beginning.get()));
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
    final Optional<DateTime> beginning = storage.beginning();
    final Optional<DateTime> end = storage.end();
    if (beginning.isPresent() && end.isPresent()) {
      Assert.assertTrue(end.get().isAfter(beginning.get()));
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

}