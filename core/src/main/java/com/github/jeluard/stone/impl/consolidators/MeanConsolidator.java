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
package com.github.jeluard.stone.impl.consolidators;

import com.github.jeluard.stone.spi.Consolidator;

import java.util.LinkedList;
import java.util.List;

/**
 * A {@link com.github.jeluard.stone.spi.Consolidator} providing the {@code mean} of accumulated values.
 */
public class MeanConsolidator extends BaseConsolidator {

  private final List<Integer> values = new LinkedList<Integer>();

  private int sum(final List<Integer> integers) {
    int sum = 0;
    for (final Integer integer : integers) {
      sum += integer;
    }
    return sum;
  }

  @Override
  public void accumulate(final long timestamp, final int value) {
    this.values.add(value);
  }

  @Override
  public int consolidate() {
    return sum(this.values) / this.values.size();
  }

  @Override
  protected void reset() {
    this.values.clear();
  }

}