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
 * Abstracts logic of consolidating values from a {@link com.github.jeluard.stone.api.Window} into a single {@code value}.
 */
public interface Consolidator {

  /**
   * Accumulate a new value that will be considered for the consolidation process.
   *
   * @param timestamp
   * @param value 
   */
  void accumulate(long timestamp, int value);

  /**
   * Atomically compute consolidated value and reset itself for next consolidation cycle.
   * <br />
   * When called {@link #accumulate(long, int)} has been called at least once.
   *
   * @return result of consolidation of all values accumulated since last call
   */
  int consolidateAndReset();

}