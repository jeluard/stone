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
import java.util.LinkedList;

import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Test;

public class BaseStorageTest {

  /*@Test
  public void shouldLastReturnAbsentWhenAllIsEmpty() throws IOException {
    final BaseStorage storage = new BaseStorage() {
      @Override
      public void append(long timestamp, int[] data) throws IOException {
      }
      @Override
      public Iterable<Pair<Long, int[]>> all() throws IOException {
        return new LinkedList<Pair<Long, int[]>>();
      }
    };

    Assert.assertFalse(storage.beginning().isPresent());
  }

  @Test
  public void shouldLastReturnPresentWhenAllReturnsOneElement() throws IOException {
    final BaseStorage storage = new BaseStorage() {
      @Override
      public void append(long timestamp, int[] data) throws IOException {
      }
      @Override
      public Iterable<Pair<Long, int[]>> all() throws IOException {
        return Arrays.asList(new Pair<Long, int[]>(DateTime.now().getMillis(), new int[0]));
      }
    };

    Assert.assertTrue(storage.beginning().isPresent());
  }*/

}
