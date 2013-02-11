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

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;

import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;

public class StorageTest {

  private Window createWindow() {
    return Window.of(Duration.standardSeconds(1)).persistedDuring(Duration.standardSeconds(10)).consolidatedBy(MaxConsolidator.class);
  }

  @Test
  public void shouldBeginningReturnAbsentWhenAllIsEmpty() throws IOException {
    final Storage storage = new Storage(createWindow()) {
      @Override
      public void onConsolidation(long timestamp, int[] data) throws IOException {
      }
      @Override
      public Iterable<Pair<Long, int[]>> all() throws IOException {
        return new LinkedList<Pair<Long, int[]>>();
      }
    };

    Assert.assertFalse(storage.beginning().isPresent());
  }

  @Test
  public void shouldBeginningReturnFirstFromAll() throws IOException {
    final long millis = DateTime.now().getMillis();
    final long millis2 = DateTime.now().plus(1).getMillis();
    final Storage storage = new Storage(createWindow()) {
      @Override
      public void onConsolidation(long timestamp, int[] data) throws IOException {
      }
      @Override
      public Iterable<Pair<Long, int[]>> all() throws IOException {
        return Arrays.asList(new Pair<Long, int[]>(millis, new int[0]), new Pair<Long, int[]>(millis2, new int[0]));
      }
    };

    Assert.assertTrue(storage.end().isPresent());
    Assert.assertEquals(millis, storage.beginning().get().getMillis());
  }

  @Test
  public void shouldEndReturnAbsentWhenAllIsEmpty() throws IOException {
    final Storage storage = new Storage(createWindow()) {
      @Override
      public void onConsolidation(long timestamp, int[] data) throws IOException {
      }
      @Override
      public Iterable<Pair<Long, int[]>> all() throws IOException {
        return new LinkedList<Pair<Long, int[]>>();
      }
    };

    Assert.assertFalse(storage.end().isPresent());
  }

  @Test
  public void shouldEndReturnLastFromAll() throws IOException {
    final long millis = DateTime.now().getMillis();
    final long millis2 = DateTime.now().plus(1).getMillis();
    final Storage storage = new Storage(createWindow()) {
      @Override
      public void onConsolidation(long timestamp, int[] data) throws IOException {
      }
      @Override
      public Iterable<Pair<Long, int[]>> all() throws IOException {
        return Arrays.asList(new Pair<Long, int[]>(millis, new int[0]), new Pair<Long, int[]>(millis2, new int[0]));
      }
    };

    Assert.assertTrue(storage.end().isPresent());
    Assert.assertEquals(millis2, storage.end().get().getMillis());
  }

  @Test
  public void shouldDuringBeBasedOnAll() throws IOException {
    final long millis = DateTime.now().getMillis();
    final long millis2 = DateTime.now().plus(1).getMillis();
    final long millis3 = DateTime.now().plus(2).getMillis();
    final long millis4 = DateTime.now().plus(3).getMillis();
    final Storage storage = new Storage(createWindow()) {
      @Override
      public void onConsolidation(long timestamp, int[] data) throws IOException {
      }
      @Override
      public Iterable<Pair<Long, int[]>> all() throws IOException {
        return Arrays.asList(new Pair<Long, int[]>(millis, new int[0]), 
                new Pair<Long, int[]>(millis2, new int[0]),
                new Pair<Long, int[]>(millis3, new int[0]),
                new Pair<Long, int[]>(millis4, new int[0]));
      }
    };

    final Iterable<Pair<Long, int[]>> iterable = storage.during(new Interval(millis2, millis3));
    final Iterator<Pair<Long, int[]>> result = iterable.iterator();
    Assert.assertEquals(millis2, (long) result.next().first);
    Assert.assertEquals(millis3, (long) result.next().first);
    Assert.assertFalse(result.hasNext());
  }

  @Test
  public void shouldDuringStopWhenAllStop() throws IOException {
    final long millis = DateTime.now().getMillis();
    final long millis2 = DateTime.now().plus(1).getMillis();
    final long millis3 = DateTime.now().plus(2).getMillis();
    final long millis4 = DateTime.now().plus(3).getMillis();
    final long millis5 = DateTime.now().plus(4).getMillis();
    final Storage storage = new Storage(createWindow()) {
      @Override
      public void onConsolidation(long timestamp, int[] data) throws IOException {
      }
      @Override
      public Iterable<Pair<Long, int[]>> all() throws IOException {
        return Arrays.asList(new Pair<Long, int[]>(millis, new int[0]), 
                new Pair<Long, int[]>(millis2, new int[0]),
                new Pair<Long, int[]>(millis3, new int[0]),
                new Pair<Long, int[]>(millis4, new int[0]));
      }
    };

    final Iterable<Pair<Long, int[]>> iterable = storage.during(new Interval(millis3, millis5));
    final Iterator<Pair<Long, int[]>> result = iterable.iterator();
    Assert.assertEquals(millis3, (long) result.next().first);
    Assert.assertEquals(millis4, (long) result.next().first);
    Assert.assertFalse(result.hasNext());
  }

  @Test
  public void shouldDuringIterableBeReusable() throws IOException {
    final long millis = DateTime.now().getMillis();
    final long millis2 = DateTime.now().plus(1).getMillis();
    final long millis3 = DateTime.now().plus(2).getMillis();
    final long millis4 = DateTime.now().plus(3).getMillis();
    final Storage storage = new Storage(createWindow()) {
      @Override
      public void onConsolidation(long timestamp, int[] data) throws IOException {
      }
      @Override
      public Iterable<Pair<Long, int[]>> all() throws IOException {
        return Arrays.asList(new Pair<Long, int[]>(millis, new int[0]), 
                new Pair<Long, int[]>(millis2, new int[0]),
                new Pair<Long, int[]>(millis3, new int[0]),
                new Pair<Long, int[]>(millis4, new int[0]));
      }
    };

    final Iterable<Pair<Long, int[]>> iterable = storage.during(new Interval(millis2, millis3));
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