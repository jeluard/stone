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
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicReference;

import org.joda.time.Interval;

public class TimeSeriesDB {

  private final Storage storage;
  private final TimeSerie[] timeSeries;
  private final Dispatcher dispatcher;
  private AtomicReference<Long> lastDateReference;
  private long beginning;

  public TimeSeriesDB(final Storage storage, final Dispatcher dispatcher, final TimeSerie ... timeSeries) throws IOException {
    this.storage = Preconditions.checkNotNull(storage, "null storage");
    this.dispatcher = Preconditions.checkNotNull(dispatcher, "null dispatcher");
    this.timeSeries = Preconditions.checkNotNull(timeSeries, "null timeSeries");
    final Optional<Interval> interval = this.storage.interval();
    if (interval.isPresent()) {
      //Storage is not empty
      this.beginning = interval.get().getStartMillis();
      this.lastDateReference = new AtomicReference<Long>(interval.get().getEndMillis());
    }
    //TODO deal with empty storage
  }

  private long checkNotBeforeLatestDataPoint(final long currentDate) {
    final long previousDate = this.lastDateReference.getAndSet(currentDate);
    if (!(currentDate > previousDate)) {
      throw new IllegalArgumentException("Provided dataPoint from <"+currentDate+"> must be more recent than <"+previousDate+">");
    }
    return previousDate;
  }

  private long windowId(final long timestamp, final long duration) {
    return (timestamp - this.beginning) / duration;
  }

  private boolean hasWindowBeenCompleted(final long currentDate, final long previousDate, final long duration) {
    final long currentWindowId = windowId(currentDate, duration);
    final long previousWindowId = windowId(previousDate, duration);
    return currentWindowId != previousWindowId;
  }

  private void accumulate(final long timestamp, final int value, final Collection<Consolidator> consolidators) {
    this.dispatcher.accumulate(timestamp, value, consolidators);
  }

  private void persist(final Collection<Consolidator> consolidators) throws IOException {
    this.storage.append(this.dispatcher.reduce(consolidators));
  }

  //https://blogs.oracle.com/dholmes/entry/inside_the_hotspot_vm_clocks
  public void publish(final long timestamp, final int value) throws IOException {
    Preconditions.checkNotNull(timestamp, "null timestamp");
    Preconditions.checkNotNull(value, "null value");

    final long previousDate = checkNotBeforeLatestDataPoint(timestamp);

    //TODO // ?
    for (final TimeSerie timeSerie : this.timeSeries) {
      final Collection<Consolidator> consolidators = timeSerie.getConsolidators();

      //TODO sort by frame, ts with same frame can be optimized
      for (final TimeSerie.SamplingFrame samplingFrame : timeSerie.getSamplingFrames()) {
        accumulate(timestamp, value, consolidators);

        if (hasWindowBeenCompleted(timestamp, previousDate, samplingFrame.getDuration())) {
          persist(consolidators);
        }
      }
    }
  }

}