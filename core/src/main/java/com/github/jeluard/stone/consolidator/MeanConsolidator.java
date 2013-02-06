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

/**
 * A {@link Consolidator} providing the {@code mean} of accumulated values.
 */
public final class MeanConsolidator extends Consolidator {

  private volatile int sum = 0;
  private volatile int count = 0;

  @Override
  public void accumulate(final long timestamp, final int value) {
    //Not atomic but not needed.
    this.sum += value;
    ++this.count;
  }

  @Override
  public int consolidate() {
    //Not atomic but not needed.
    return this.sum / this.count;
  }

  @Override
  protected void reset() {
    //Not atomic but not needed.
    this.sum = 0;
    this.count = 0;
  }

}