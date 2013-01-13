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
package com.github.jeluard.stone.spi;

import com.github.jeluard.guayaba.annotation.Idempotent;
import com.github.jeluard.stone.api.DataAggregates;
import java.util.Collection;

/**
 * Dispatches {@link DataPoint}s to currently configured {@link Consolidator}.
 * <br>
 * Adding/removing {@link Consolidator}s during accumulation phasis might lead to incoherent results.
 */
public interface Dispatcher {

  /**
   *
   * At this point both parameters have been validated and are not null.
   *
   * @param timestamp
   * @param value 
   */
  void accumulate(long timestamp, int value, Collection<Consolidator> consolidators);

  int[] reduce(Collection<Consolidator> consolidators);

}