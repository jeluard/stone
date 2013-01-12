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
package com.github.jeluard.stone.impl;

import com.github.jeluard.stone.api.DataAggregates;
import com.github.jeluard.stone.spi.BaseDispatcher;
import com.github.jeluard.stone.spi.Consolidator;
import com.github.jeluard.stone.spi.Dispatcher;
import com.google.common.base.Preconditions;

import java.util.ArrayList;
import java.util.List;

import org.joda.time.DateTime;

/**
 * {@link Dispatcher} implementation executing {@link Consolidator#accumulate(com.github.jeluard.stone.api.DataPoint)} in the caller thread.
 */
public class SequentialDispatcher extends BaseDispatcher {

  @Override
  public void accumulate(final long timestamp, final int value) {
    Preconditions.checkNotNull(timestamp, "null timestamp");
    Preconditions.checkNotNull(value, "null value");

    for (final Consolidator consolidator : getConsolidators()) {
      consolidator.accumulate(timestamp, value);
    }
  }

  @Override
  public DataAggregates reduce() {
    final List<Consolidator> consolidators = getConsolidators();
    final List<Long> integers = new ArrayList<Long>(consolidators.size());
    for (final Consolidator consolidator : consolidators) {
      integers.add(consolidator.consolidateAndReset());
    }
    return new DataAggregates(DateTime.now(), integers);
  }

}