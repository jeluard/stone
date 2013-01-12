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
  private final AtomicReference<Long> lastDateReference;
  private long beginning;

  public TimeSeries(final Duration windowDuration, final Dispatcher dispatcher, final Storage storage, final Consolidator ... consolidators) throws IOException {
    this.windowDurationInMillis = Preconditions.checkNotNull(windowDuration, "null windowDuration").getMillis();
    Preconditions.checkArgument(this.windowDurationInMillis > 0, "windowDuration must be positive");
    this.storage = Preconditions.checkNotNull(storage, "null storage");
    this.dispatcher = Preconditions.checkNotNull(dispatcher, "null dispatcher");
    final DateTime lastStorageDate = this.storage.last().or(DateTime.now());
    this.beginning = lastStorageDate.getMillis();
    this.lastDateReference = new AtomicReference<Long>(lastStorageDate.getMillis());

    Preconditions.checkNotNull(consolidators, "null consolidators");
    for (final Consolidator consolidator : consolidators) {
      this.dispatcher.addConsolidator(consolidator);
    }
  }

  private long checkNotBeforeLatestDataPoint(final long currentDate) {
    final long previousDate = this.lastDateReference.getAndSet(currentDate);
    if (!(currentDate > previousDate)) {
      throw new IllegalArgumentException("Provided dataPoint from <"+currentDate+"> must be more recent than <"+previousDate+">");
    }
    return previousDate;
  }

  private long windowId(final long timestamp) {
    return (timestamp - this.beginning) / this.windowDurationInMillis;
  }

  private boolean hasWindowBeenCompleted(final long currentDate, final long previousDate) {
    final long currentWindowId = windowId(currentDate);
    final long previousWindowId = windowId(previousDate);
    return currentWindowId != previousWindowId;
  }

  private void accumulate(final long timestamp, final int value) {
    this.dispatcher.accumulate(timestamp, value);
  }

  private void persist() throws IOException {
    this.storage.append(this.dispatcher.reduce());
  }

  //https://blogs.oracle.com/dholmes/entry/inside_the_hotspot_vm_clocks
  public void publish(final long timestamp, final int value) throws IOException {
    Preconditions.checkNotNull(timestamp, "null timestamp");
    Preconditions.checkNotNull(value, "null value");

    final long previousDate = checkNotBeforeLatestDataPoint(timestamp);

    accumulate(timestamp, value);
    if (hasWindowBeenCompleted(timestamp, previousDate)) {
      persist();
    }
  }

}