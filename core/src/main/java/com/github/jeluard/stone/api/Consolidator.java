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
package com.github.jeluard.stone.api;

/**
 * Abstracts logic of consolidating values from a {@link Window} into a single {@code value}.
 * <br>
 * {@link Consolidator} must define one of following constructors:
 * <ul>
 *   <li>a constructor accepting an {@code int} as unique constructor (hinting the maximum number of samples in a {@link Window})</li>
 *   <li>default constructor</li>
 * </ul>
 * First one will be prefered.
 * <br>
 * <br>
 * A {@link Consolidator} will be instantiated per {@link TimeSeries} / {@link Window} couple.
 * <br>
 * <br>
 * Different threads might be used to call successive {@link #accumulate(long, int)} and {@link #consolidateAndReset()} but <b>never</b> concurrently.
 * <br>
 * For a considered {@link Window} duration {@link #consolidateAndReset()} will *always* be called after all {@link #accumulate(long, int)}s. Next {@link #accumulate(long, int)} call is performed after {@link #consolidateAndReset()} is executed.
 * {@link #accumulate(long, int)} calls can be re-ordered (i.e. {@code timestamp} is not monotonically increasing).
 * <br>
 * A {@link Consolidator} instance will never receive the same timestamp twice.
 */
public abstract class Consolidator {

  /**
   * Accumulate a new value that will be considered for the consolidation process.
   *
   * @param timestamp
   * @param value 
   */
  public abstract void accumulate(long timestamp, int value);

  /**
   * Atomically compute consolidated value and reset itself for next consolidation cycle.
   * <br />
   * When called {@link #accumulate(long, int)} has been called at least once.
   *
   * @return result of consolidation of all values accumulated since last call
   */
  public final int consolidateAndReset() {
    final int consolidate = consolidate();
    reset();
    return consolidate;
  }

  /**
   * Called right after {@link #consolidate()}.
   */
  protected abstract void reset();

  /**
   * @return final consolidated value
   */
  protected abstract int consolidate();

}