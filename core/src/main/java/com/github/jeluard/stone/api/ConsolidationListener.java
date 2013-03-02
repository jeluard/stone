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
 * A listener to hook the consolidation process.
 */
public interface ConsolidationListener {

  public interface Persistent extends ConsolidationListener {

    /**
     * @return latest (most recent) timestamp persisted
     */
    long getLatestTimestamp();

  }

  /**
   * Invoked each time a newly published value cross a {@link Window} boundary triggering the consolidation process.
   * <br>
   * Different thread might invoke this method but never concurrently. Consecutive {@code timestamp} values are guaranteed to be monotonically increasing.
   *
   * @param timestamp
   * @param consolidates 
   */
  void onConsolidation(long timestamp, int[] consolidates);

}