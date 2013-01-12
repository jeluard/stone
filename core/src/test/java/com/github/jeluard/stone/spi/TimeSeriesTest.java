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

import com.github.jeluard.stone.api.DataAggregates;
import com.github.jeluard.stone.api.DataPoint;
import com.github.jeluard.stone.api.TimeSeries;
import com.google.common.base.Optional;

import java.io.IOException;

import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.Period;
import org.junit.Test;
import org.mockito.Mockito;

public class TimeSeriesTest {

  private Storage createStorageMock(final Optional<DateTime> last) throws IOException {
    final Storage mock = Mockito.mock(Storage.class);
    Mockito.when(mock.last()).thenReturn(last);
    return mock;
  }

  private Storage createStorageMock() throws IOException {
    return createStorageMock(Optional.<DateTime>absent());
  }

  @Test(expected=IllegalArgumentException.class)
  public void shouldOldDataPointBeRejected() throws IOException {
    final DateTime last = DateTime.now();
    final TimeSeries timeSeries = new TimeSeries(Duration.ZERO, Mockito.mock(Dispatcher.class), createStorageMock(Optional.of(last)), Mockito.mock(Consolidator.class));
    final DateTime now = DateTime.now();
    timeSeries.publish(new DataPoint(now, 0));
    timeSeries.publish(new DataPoint(now, 0));
  }

  @Test(expected=IllegalArgumentException.class)
  public void shouldDataPointWithSameDateAsStorageLastBeRejected() throws IOException {
    final DateTime now = DateTime.now();
    final TimeSeries timeSeries = new TimeSeries(Duration.standardMinutes(2), Mockito.mock(Dispatcher.class), createStorageMock(Optional.of(now)), Mockito.mock(Consolidator.class));
    timeSeries.publish(new DataPoint(now, 0));
  }

  @Test
  public void shouldStorageAppendBeInvokedOnceWhenOnePeriodIsCrossed() throws IOException {
    final DateTime now = DateTime.now();
    final Storage storage = createStorageMock(Optional.of(now));
    final TimeSeries timeSeries = new TimeSeries(Duration.standardMinutes(2), Mockito.mock(Dispatcher.class), storage, Mockito.mock(Consolidator.class));
    timeSeries.publish(new DataPoint(now.plus(Period.millis(1)), 0));
    timeSeries.publish(new DataPoint(now.plus(Period.millis(2)), 0));
    timeSeries.publish(new DataPoint(now.plus(Period.millis(5)), 0));

    Mockito.verify(storage).append(Mockito.<DataAggregates>any());
  }

  @Test
  public void shouldStorageAppendBeInvokedTwiceWhenTwoPeriodIsCrossed() throws IOException {
    final DateTime now = DateTime.now();
    final Storage storage = createStorageMock(Optional.of(now));
    final TimeSeries timeSeries = new TimeSeries(Duration.standardMinutes(2), Mockito.mock(Dispatcher.class), storage, Mockito.mock(Consolidator.class));
    timeSeries.publish(new DataPoint(now.plus(Period.millis(1)), 0));
    timeSeries.publish(new DataPoint(now.plus(Period.millis(5)), 0));
    timeSeries.publish(new DataPoint(now.plus(Period.millis(10)), 0));

    Mockito.verify(storage, Mockito.times(2)).append(Mockito.<DataAggregates>any());
  }

}