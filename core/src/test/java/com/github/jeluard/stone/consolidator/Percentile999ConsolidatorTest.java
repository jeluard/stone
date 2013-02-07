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
package com.github.jeluard.stone.consolidator;

import com.github.jeluard.stone.api.ConsolidatorTest;

import org.junit.Assert;
import org.junit.Test;

public class Percentile999ConsolidatorTest extends ConsolidatorTest<Percentile999Consolidator> {

  @Override
  protected Class<Percentile999Consolidator> getType() {
    return Percentile999Consolidator.class;
  }

  @Test
  public void shouldResultBeCorrect() {
    final Percentile999Consolidator consolidator = new Percentile999Consolidator(1000);
    for (int i = 0; i < 1000; i++) {
      consolidator.accumulate(i, i);
    }

    Assert.assertEquals(999, consolidator.consolidateAndReset());
  }

}