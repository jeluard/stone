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

public class Percentile90ConsolidatorTest extends ConsolidatorTest<Percentile90Consolidator> {

  @Override
  protected Class<Percentile90Consolidator> getType() {
    return Percentile90Consolidator.class;
  }

  @Test
  public void shouldResultBeCorrect() {
    final Percentile90Consolidator consolidator = new Percentile90Consolidator(100);
    for (int i = 0; i < 100; i++) {
      consolidator.accumulate(i, i);
    }

    Assert.assertEquals(90, consolidator.consolidateAndReset());
  }

}