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

import com.github.jeluard.guayaba.util.concurrent.ExecutorServices;
import com.github.jeluard.stone.api.DataAggregates;
import com.github.jeluard.stone.api.DataPoint;
import com.github.jeluard.stone.spi.BaseDispatcher;
import com.github.jeluard.stone.spi.Consolidator;
import com.github.jeluard.stone.spi.Dispatcher;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Collections2;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;

import org.joda.time.DateTime;

/**
 * {@link Dispatcher} implementation relying on an {@link ExecutorService} to execute {@link Consolidator#accumulate(com.github.jeluard.stone.api.DataPoint)}.
 * <br>
 * {@link ExecutorService} lyfecycle is *not* managed by this class.
 */
public class ExecutorDispatcher extends BaseDispatcher {

  private static final class AccumulatorRunnable implements Runnable {

    private final Consolidator consolidator;
    private final DataPoint dataPoint;

    public AccumulatorRunnable(final Consolidator consolidator, final DataPoint dataPoint) {
      this.consolidator = consolidator;
      this.dataPoint = dataPoint;
    }

    @Override
    public void run() {
      this.consolidator.accumulate(this.dataPoint);
    }

  }

  private final ExecutorService executor;

  public ExecutorDispatcher(final ExecutorService executor) {
    this.executor = Preconditions.checkNotNull(executor, "null executor");
  }

  @Override
  public void accumulate(final DataPoint dataPoint) {
    Preconditions.checkNotNull(dataPoint, "null dataPoint");

    try {
      ExecutorServices.invokeAll(this.executor, Collections2.transform(getConsolidators(), new Function<Consolidator, AccumulatorRunnable>() {
        @Override
        public AccumulatorRunnable apply(final Consolidator input) {
          Preconditions.checkNotNull(input, "null input");

          return new AccumulatorRunnable(input, dataPoint);
        }
      }));
    } catch (InterruptedException e) {
      // Preserve interrupt status
      Thread.currentThread().interrupt();
    }
  }

  @Override
  public DataAggregates reduce() {
    final List<Consolidator> consolidators = getConsolidators();
    final List<Integer> integers = new ArrayList<Integer>(consolidators.size());
    for (final Consolidator consolidator : consolidators) {
      integers.add(consolidator.consolidateAndReset().getValue());
    }
    return new DataAggregates(DateTime.now(), integers);
  }

}