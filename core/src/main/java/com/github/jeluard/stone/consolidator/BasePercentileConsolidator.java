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

import com.github.jeluard.guayaba.base.Preconditions2;
import com.github.jeluard.stone.api.Consolidator;
import com.google.common.base.Preconditions;
import com.google.common.primitives.Ints;

import java.util.concurrent.atomic.AtomicIntegerArray;

/**
 * A {@link Consolidator} providing the pth {@code percentile} of accumulated values.
 * <br>
 * <b>nearest rank</b> algorithm is used here.
 * <br>
 * A integer array of {@link com.github.jeluard.stone.api.Window#getMaxSamples()} will be allocated to limit array resizing. This leads to high memory usage.
 */
public abstract class BasePercentileConsolidator extends Consolidator {

  private final int maxSamples;
  private final float pth;
  private volatile int index = 0;
  private AtomicIntegerArray values;//rely on happens-before ordering to avoid volatile modifier

  public BasePercentileConsolidator(final int maxSamples, final float pth) {
    Preconditions.checkArgument(pth > 0 && pth < 100, "pth must be > 0 and < 100");
    this.maxSamples = maxSamples;
    this.pth = pth;
    this.values = new AtomicIntegerArray(Preconditions2.checkSize(maxSamples));
  }

  private int rank(final float p, final int size) {
    return Ints.saturatedCast(Math.round(p / 100 * size +.5));
  }

  @Override
  public final void accumulate(final long timestamp, final int value) {
    //Not atomic but not needed.
    this.values.set(this.index++, value);//volatile access on index, force to see latest content for values
  }

  @Override
  public final int consolidate() {
    //Not atomic but not needed.
    final int rank = Math.max(0, Math.min(rank(this.pth, this.index) - 1, this.index));//volatile access on index, force to see latest content for values
    return this.values.get(rank);
  }

  @Override
  protected void reset() {
    //Not atomic but not needed.
    this.values = new AtomicIntegerArray(this.maxSamples);
    this.index = 0;//Make sure index is written *after* values, making values content available to any thread after reading index
  }

}