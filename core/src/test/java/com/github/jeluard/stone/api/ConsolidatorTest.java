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
package com.github.jeluard.stone.api;

import java.lang.reflect.Constructor;

import org.junit.Assert;
import org.junit.Test;

public abstract class ConsolidatorTest<T extends Consolidator> {

  protected abstract Class<T> getType();

  protected T createInstance() {
    try {
      final Class<T> type = getType();
      try {
        type.getConstructor();
        return getType().newInstance();
      } catch (NoSuchMethodException e) {
        final Constructor<T> intConstructor = type.getConstructor(int.class);
        return intConstructor.newInstance(5);
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void shouldDefineAcceptableConstructor() {
    createInstance();
  }

  @Test
  public void shouldConsolidationBeSuccessful() {
    final Consolidator consolidator = createInstance();
    consolidator.accumulate(1L, 1);
    consolidator.accumulate(2L, 2);
    consolidator.accumulate(2L, 1);
    consolidator.consolidateAndReset();
  }

  @Test
  public void shouldConsolidationBeReproducible() {
    final Consolidator consolidator = createInstance();
    consolidator.accumulate(1L, 1);
    consolidator.accumulate(2L, 2);
    final int consolidate1 = consolidator.consolidateAndReset();
    consolidator.accumulate(1L, 1);
    consolidator.accumulate(2L, 2);
    final int consolidate2 = consolidator.consolidateAndReset();

    Assert.assertEquals(consolidate1, consolidate2);
  }

  @Test
  public void shouldBeThreadSafe() throws InterruptedException {
    final Consolidator consolidator = createInstance();
    final Thread thread = new Thread(new Runnable() {
      @Override
      public void run() {
        consolidator.accumulate(1L, 1);
        consolidator.accumulate(2L, 2);
      }
    });
    thread.start();
    thread.join();
    final int consolidate1 = consolidator.consolidateAndReset();
    consolidator.accumulate(1L, 1);
    consolidator.accumulate(2L, 2);
    final int consolidate2 = consolidator.consolidateAndReset();

    Assert.assertEquals(consolidate1, consolidate2);
  }

}