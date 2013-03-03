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

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;

import org.junit.Assert;
import org.junit.Test;

public class StorageTest {

  private Storage createStorage(final int maximumSize, final Iterable<Pair<Long, int[]>> all) {
    return new Storage(maximumSize) {
      @Override
      public void append(long timestamp, int[] data) throws IOException {
      }
      @Override
      public Iterable<Pair<Long, int[]>> all() throws IOException {
        return all;
      }
    };
  }

  @Test
  public void shouldMaximumSizeBeAccessible() throws IOException {
    final int maximumSize = 1000;
    final Storage storage = createStorage(maximumSize, new LinkedList<Pair<Long, int[]>>());

    Assert.assertEquals(maximumSize, storage.getMaximumSize());
  }

  @Test
  public void shouldBeginningReturnAbsentWhenAllIsEmpty() throws IOException {
    final Storage storage = createStorage(1000, new LinkedList<Pair<Long, int[]>>());

    Assert.assertFalse(storage.beginning().isPresent());
  }

  @Test
  public void shouldBeginningReturnFirstFromAll() throws IOException {
    final long millis = System.currentTimeMillis();
    final long millis2 = millis + 1;
    final Storage storage = createStorage(1000, Arrays.asList(new Pair<Long, int[]>(millis, new int[0]), new Pair<Long, int[]>(millis2, new int[0])));

    Assert.assertTrue(storage.end().isPresent());
    Assert.assertEquals(millis, (long) storage.beginning().get());
  }

  @Test
  public void shouldEndReturnAbsentWhenAllIsEmpty() throws IOException {
    final Storage storage = createStorage(1000, new LinkedList<Pair<Long, int[]>>());

    Assert.assertFalse(storage.end().isPresent());
  }

  @Test
  public void shouldEndReturnLastFromAll() throws IOException {
    final long millis = System.currentTimeMillis();
    final long millis2 = millis + 1;
    final Storage storage = createStorage(1000, Arrays.asList(new Pair<Long, int[]>(millis, new int[0]), new Pair<Long, int[]>(millis2, new int[0])));

    Assert.assertTrue(storage.end().isPresent());
    Assert.assertEquals(millis2, (long) storage.end().get());
  }

  @Test
  public void shouldDuringBeBasedOnAll() throws IOException {
    final long millis = System.currentTimeMillis();
    final long millis2 = millis + 1;
    final long millis3 = millis + 2;
    final long millis4 = millis + 3;
    final Storage storage = createStorage(1000, Arrays.asList(new Pair<Long, int[]>(millis, new int[0]), 
                new Pair<Long, int[]>(millis2, new int[0]),
                new Pair<Long, int[]>(millis3, new int[0]),
                new Pair<Long, int[]>(millis4, new int[0])));

    final Iterable<Pair<Long, int[]>> iterable = storage.during(millis2, millis3);
    final Iterator<Pair<Long, int[]>> result = iterable.iterator();
    Assert.assertEquals(millis2, (long) result.next().first);
    Assert.assertEquals(millis3, (long) result.next().first);
    Assert.assertFalse(result.hasNext());
  }

  @Test
  public void shouldDuringStopWhenAllStop() throws IOException {
    final long millis = System.currentTimeMillis();
    final long millis2 = millis + 1;
    final long millis3 = millis + 2;
    final long millis4 = millis + 3;
    final long millis5 = millis + 4;
    final Storage storage = createStorage(1000, Arrays.asList(new Pair<Long, int[]>(millis, new int[0]), 
                new Pair<Long, int[]>(millis2, new int[0]),
                new Pair<Long, int[]>(millis3, new int[0]),
                new Pair<Long, int[]>(millis4, new int[0])));

    final Iterable<Pair<Long, int[]>> iterable = storage.during(millis3, millis5);
    final Iterator<Pair<Long, int[]>> result = iterable.iterator();
    Assert.assertEquals(millis3, (long) result.next().first);
    Assert.assertEquals(millis4, (long) result.next().first);
    Assert.assertFalse(result.hasNext());
  }

  @Test
  public void shouldDuringIterableBeReusable() throws IOException {
    final long millis = System.currentTimeMillis();
    final long millis2 = millis + 1;
    final long millis3 = millis + 2;
    final long millis4 = millis + 3;
    final Storage storage = createStorage(1000, Arrays.asList(new Pair<Long, int[]>(millis, new int[0]), 
                new Pair<Long, int[]>(millis2, new int[0]),
                new Pair<Long, int[]>(millis3, new int[0]),
                new Pair<Long, int[]>(millis4, new int[0])));

    final Iterable<Pair<Long, int[]>> iterable = storage.during(millis2, millis3);
    final Iterator<Pair<Long, int[]>> result = iterable.iterator();
    Assert.assertEquals(millis2, (long) result.next().first);
    Assert.assertEquals(millis3, (long) result.next().first);
    Assert.assertFalse(result.hasNext());

    final Iterator<Pair<Long, int[]>> result2 = iterable.iterator();
    Assert.assertEquals(millis2, (long) result2.next().first);
    Assert.assertEquals(millis3, (long) result2.next().first);
    Assert.assertFalse(result.hasNext());
  }

}