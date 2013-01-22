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

import com.github.jeluard.stone.api.Window;
import com.google.common.base.Preconditions;
import com.google.common.primitives.Ints;

import java.util.Arrays;

/**
 * A {@link com.github.jeluard.stone.spi.Consolidator} providing the pth {@code percentile} of accumulated values.
 * <br>
 * <b>nearest rank</b> algorithm is used here.
 * <br>
 * A integer array of {@link Window#getMaxSamples()} will be allocated to limit array resizing. This leads to high memory usage.
 */
public abstract class PercentileConsolidator extends BaseConsolidator {

  private final float pth;
  private final int[] values;
  private int index = 0;

  public PercentileConsolidator(final int maxSamples, final float pth) {
    Preconditions.checkArgument(pth > 0 && pth < 100, "pth must be > 0 and < 100");
    this.pth = pth;
    this.values = new int[maxSamples];
  }

  private int rank(final float p, final int[] integers) {
    return Ints.saturatedCast(Math.round(p / 100 * integers.length +.5));
  }

  @Override
  public void accumulate(final long timestamp, final int value) {
    this.values[this.index++] = value;
  }

  @Override
  public int consolidate() {
    final int rank = Math.max(0, Math.min(rank(this.pth, this.values) - 1, this.values.length));
    return this.values[rank];
  }

  @Override
  protected void reset() {
    this.index = 0;
    Arrays.fill(this.values, 0);
  }

}