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

import com.github.jeluard.stone.api.Consolidator;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;
import com.google.common.collect.Ordering;
import com.google.common.primitives.Ints;

/**
 * A {@link Consolidator} providing the {@code mode} of accumulated values.
 */
public final class ModeConsolidator extends Consolidator {

  private final Multiset<Integer> values;
  //Inspired from guava internals
  private static final Ordering<Multiset.Entry<?>> DECREASING_COUNT_ORDERING = new Ordering<Multiset.Entry<?>>() {
    @Override
    public int compare(final Multiset.Entry<?> entry1, final Multiset.Entry<?> entry2) {
      return Ints.compare(entry1.getCount(), entry2.getCount());
    }
  };

  public ModeConsolidator(final int maxSamples) {
    this.values = HashMultiset.create(maxSamples);
  }

  @Override
  public synchronized void accumulate(final long timestamp, final int value) {
    this.values.add(value);
  }

  @Override
  public synchronized int consolidate() {
    return ModeConsolidator.DECREASING_COUNT_ORDERING.greatestOf(this.values.entrySet(), 1).get(0).getElement();
  }

  @Override
  protected synchronized void reset() {
    this.values.clear();
  }

}