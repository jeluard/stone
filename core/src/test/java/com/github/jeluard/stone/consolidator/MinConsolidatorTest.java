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

public class MinConsolidatorTest extends ConsolidatorTest<MinConsolidator> {

  @Override
  protected Class<MinConsolidator> getType() {
    return MinConsolidator.class;
  }

  @Test
  public void shouldResultBeCorrect() {
    final MinConsolidator consolidator = new MinConsolidator();
    consolidator.accumulate(2L, 1);
    consolidator.accumulate(1L, 2);
    consolidator.accumulate(4L, 3);
    consolidator.accumulate(3L, 4);

    Assert.assertEquals(1, consolidator.consolidateAndReset());
  }

}