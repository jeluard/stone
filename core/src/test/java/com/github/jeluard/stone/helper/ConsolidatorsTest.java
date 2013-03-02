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
package com.github.jeluard.stone.helper;

import com.github.jeluard.guayaba.test.AbstractHelperClassTest;
import com.github.jeluard.guayaba.test.junit.LoggerRule;
import com.github.jeluard.stone.api.Consolidator;
import com.github.jeluard.stone.consolidator.MaxConsolidator;
import com.github.jeluard.stone.consolidator.MinConsolidator;
import com.github.jeluard.stone.consolidator.Percentile90Consolidator;

import java.util.Arrays;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

public class ConsolidatorsTest extends AbstractHelperClassTest {

  static class ConsolidatorWithInvalidConstructor extends Consolidator {
    public ConsolidatorWithInvalidConstructor(float argument) {
    }
    @Override
    public void accumulate(long timestamp, int value) {
    }
    @Override
    protected int consolidate() {
      return 0;
    }
    @Override
    protected void reset() {
    }
  }

  static class ConsolidatorWithFailingConstructor extends Consolidator {
    public ConsolidatorWithFailingConstructor() {
      throw new RuntimeException();
    }
    @Override
    public void accumulate(long timestamp, int value) {
    }
    @Override
    protected int consolidate() {
      return 0;
    }

    @Override
    protected void reset() {
    }
  }

  @Rule
  public final LoggerRule loggerRule = new LoggerRule(Loggers.BASE_LOGGER);

  @Override
  protected Class<?> getType() {
    return Consolidators.class;
  }

  @Test
  public void shouldConsolidatorsBeSuccessful() {
    Assert.assertEquals(2, Consolidators.createConsolidators(Arrays.asList(MinConsolidator.class, MaxConsolidator.class), 1).length);
  }

  @Test
  public void shouldCreationWithConsolidatorDefiningDefaultConstructorBeSuccessful() {
    Consolidators.createConsolidator(MaxConsolidator.class, 1);
  }

  @Test
  public void shouldCreationWithConsolidatorDefiningIntConstructorBeSuccessful() {
    Consolidators.createConsolidator(Percentile90Consolidator.class, 1);
  }

  @Test(expected=IllegalArgumentException.class)
  public void shouldCreationWithConsolidatorDefiningInvalidConstructorBeInvalid() {
    Consolidators.createConsolidator(ConsolidatorWithInvalidConstructor.class, 1);
  }

  @Test(expected=RuntimeException.class)
  public void shouldCreationWithConsolidatorDefiningFailingConstructorBeInvalid() {
    Consolidators.createConsolidator(ConsolidatorWithFailingConstructor.class, 1);
  }

}