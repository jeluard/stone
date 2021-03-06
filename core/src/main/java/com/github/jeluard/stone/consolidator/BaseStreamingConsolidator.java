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
 * Base implementation for {@link Consolidator} consolidating after each accumulation.
 * <br/>
 * Live consolidators eventually compromise efficiency (as result is computed at each accumulation) for memory usage (as all elements don't have to be kept).
 */
public abstract class BaseStreamingConsolidator extends Consolidator {

  private volatile int currentResult = initialValue();

  /**
   * @return value used as basis for {@link #getCurrentResult()}
   */
  protected int initialValue() {
    return 0;
  }

  /**
   * @return current consolidated value
   */
  protected final int getCurrentResult() {
    return this.currentResult;
  }

  /**
   * Set new consolidated value.
   *
   * @param value 
   */
  protected final void setCurrentResult(final int value) {
    this.currentResult = value;
  }

  @Override
  protected final int consolidate() {
    return this.currentResult;
  }

  @Override
  protected final void reset() {
    this.currentResult = initialValue();

    afterReset();
  }

  /**
   * Hook allowing to react <b>after</b> {@link #getCurrentResult()} is reset.
   */
  protected void afterReset() {
  }

}