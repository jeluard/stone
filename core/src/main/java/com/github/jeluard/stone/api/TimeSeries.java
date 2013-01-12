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

import com.github.jeluard.stone.spi.Consolidator;
import com.github.jeluard.stone.spi.Dispatcher;
import com.github.jeluard.stone.spi.Storage;
import com.google.common.base.Preconditions;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

import org.joda.time.DateTime;
import org.joda.time.Duration;

/**
 * A {@link TimeSeries} is a  of {@link DateTime} / {@link DataPoint} pairs. Encapsulating data logically belongs to a single source.
 */
public class TimeSeries {

  private final long windowDurationInMillis;
  private final Storage storage;
  private final Dispatcher dispatcher;
  private final AtomicReference<DateTime> lastDateReference;
  private final AtomicReference<DateTime> previousWindowCompletionDateReference;

  public TimeSeries(final Duration windowDuration, final Dispatcher dispatcher, final Storage storage, final Consolidator ... consolidators) throws IOException {
    this.windowDurationInMillis = Preconditions.checkNotNull(windowDuration, "null windowDuration").getMillis();
    Preconditions.checkArgument(this.windowDurationInMillis > 0, "windowDuration must be positive");
    this.storage = Preconditions.checkNotNull(storage, "null storage");
    this.dispatcher = Preconditions.checkNotNull(dispatcher, "null dispatcher");
    final DateTime lastStorageDate = this.storage.last().or(DateTime.now());
    this.lastDateReference = new AtomicReference<DateTime>(lastStorageDate);
    this.previousWindowCompletionDateReference = new AtomicReference<DateTime>(lastStorageDate);

    Preconditions.checkNotNull(consolidators, "null consolidators");
    for (final Consolidator consolidator : consolidators) {
      this.dispatcher.addConsolidator(consolidator);
    }
  }

  private void checkNotBeforeLatestDataPoint(final DateTime currentDate) {
    final DateTime previousDate = this.lastDateReference.getAndSet(currentDate);
    if (!currentDate.isAfter(previousDate)) {
      throw new IllegalArgumentException("Provided dataPoint from <"+currentDate+"> must be more recent than <"+previousDate+">");
    }
  }

  private synchronized boolean hasWindowBeenCompleted(final DateTime currentDate) {
    final DateTime previousWindowCompletionDate = this.previousWindowCompletionDateReference.get();
    final long duration = currentDate.getMillis() - previousWindowCompletionDate.getMillis();
    final boolean hasBeenCompleted = duration > this.windowDurationInMillis;
    if (hasBeenCompleted) {
      this.previousWindowCompletionDateReference.set(currentDate);
    }
    return hasBeenCompleted;
  }

  //https://blogs.oracle.com/dholmes/entry/inside_the_hotspot_vm_clocks
  public void publish(final DataPoint dataPoint) throws IOException {
    Preconditions.checkNotNull(dataPoint, "null dataPoint");
    checkNotBeforeLatestDataPoint(dataPoint.getDate());

    this.dispatcher.accumulate(dataPoint);
    if (hasWindowBeenCompleted(dataPoint.getDate())) {
      System.out.println("reduce...");
      this.storage.append(this.dispatcher.reduce());
    }
  }

}