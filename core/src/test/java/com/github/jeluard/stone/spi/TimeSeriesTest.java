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

import com.github.jeluard.stone.api.Archive;
import com.github.jeluard.stone.api.TimeSeries;
import com.google.common.base.Optional;

import java.io.IOException;
import java.util.Collections;

import org.joda.time.DateTime;
import org.junit.Test;
import org.mockito.Mockito;

public class TimeSeriesTest {

  private Storage createStorageMock(final Optional<DateTime> date) throws IOException {
    final Storage mock = Mockito.mock(Storage.class);
    Mockito.when(mock.beginning()).thenReturn(date);
    return mock;
  }

  @Test(expected=IllegalArgumentException.class)
  public void shouldDuplicatedTimeSeriesIDBeInvalid() throws IOException {
    final String id = "id";
    new TimeSeries(id, Collections.<Archive>emptyList(), Mockito.mock(Dispatcher.class), Mockito.mock(StorageFactory.class));
    new TimeSeries(id, Collections.<Archive>emptyList(), Mockito.mock(Dispatcher.class), Mockito.mock(StorageFactory.class));
  }

  /*@Test(expected=IllegalArgumentException.class)
  public void shouldOldDataPointBeRejected() throws IOException {
    final DateTime last = DateTime.now();
    final TimeSeries timeSeries = new TimeSeries(Duration.ZERO, Mockito.mock(Dispatcher.class), createStorageMock(Optional.of(last)), Mockito.mock(Consolidator.class));
    final long now = System.currentTimeMillis();
    timeSeries.publish(now, 0);
    timeSeries.publish(now, 0);
  }

  @Test(expected=IllegalArgumentException.class)
  public void shouldDataPointWithSameDateAsStorageLastBeRejected() throws IOException {
    final long now = System.currentTimeMillis();
    final TimeSeries timeSeries = new TimeSeries(Duration.standardSeconds(2), Mockito.mock(Dispatcher.class), createStorageMock(Optional.of(DateTime.now())), Mockito.mock(Consolidator.class));
    timeSeries.publish(now, 0);
  }

  @Test
  public void shouldStorageAppendBeInvokedOnceWhenOnePeriodIsCrossed() throws IOException {
    final DateTime now = DateTime.now();
    final Storage storage = createStorageMock(Optional.of(now));
    final TimeSeries timeSeries = new TimeSeries(Duration.standardSeconds(2), Mockito.mock(Dispatcher.class), storage, Mockito.mock(Consolidator.class));
    timeSeries.publish(now.plus(Period.seconds(1)).getMillis(), 0);
    timeSeries.publish(now.plus(Period.seconds(2)).getMillis(), 0);

    Mockito.verify(storage).append(Mockito.<int[]>any());
  }

  @Test
  public void shouldStorageAppendBeInvokedTwiceWhenTwoPeriodIsCrossed() throws IOException {
    final DateTime now = DateTime.now();
    final Storage storage = createStorageMock(Optional.of(now));
    final TimeSeries timeSeries = new TimeSeries(Duration.standardSeconds(2), Mockito.mock(Dispatcher.class), storage, Mockito.mock(Consolidator.class));
    timeSeries.publish(now.plus(Period.seconds(1)).getMillis(), 0);
    timeSeries.publish(now.plus(Period.seconds(2)).getMillis(), 0);

    Mockito.verify(storage).append(Mockito.<int[]>any());

    timeSeries.publish(now.plus(Period.seconds(3)).getMillis(), 0);
    timeSeries.publish(now.plus(Period.seconds(4)).getMillis(), 0);

    Mockito.verify(storage, Mockito.times(2)).append(Mockito.<int[]>any());
  }*/

}