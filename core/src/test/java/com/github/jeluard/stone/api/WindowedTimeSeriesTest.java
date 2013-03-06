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

import com.github.jeluard.stone.consolidator.MaxConsolidator;
import com.github.jeluard.stone.spi.Dispatcher;

import java.io.IOException;
import java.util.Arrays;

import org.junit.Test;
import org.mockito.Mockito;

public class WindowedTimeSeriesTest {

  private static class DumbDispatcher extends Dispatcher {
    public DumbDispatcher() {
      super(Dispatcher.DEFAULT_EXCEPTION_HANDLER);
    }
    @Override
    public boolean dispatch(long previousTimestamp, long currentTimestamp, int value, Listener[] listeners) {
      for (final Listener listener : listeners) {
        listener.onPublication(previousTimestamp, currentTimestamp, value);
      }
      return true;
    }
  }

  @Test
  public void shouldWindowWithoutListenerDoNothing() throws IOException {
    final Window window = Window.of(10).consolidatedBy(MaxConsolidator.class);
    final WindowedTimeSeries timeSeries = new WindowedTimeSeries("id", 1, Arrays.asList(window), new DumbDispatcher());
    timeSeries.publish(1, 1);
    timeSeries.close();
  }

  @Test
  public void shouldConsolidationBeTriggeredWhenLastElementOfWindowIsPublished() throws IOException {
    final ConsolidationListener consolidationListener = Mockito.mock(ConsolidationListener.class);
    final Window window = Window.of(2).listenedBy(consolidationListener).consolidatedBy(MaxConsolidator.class);
    final WindowedTimeSeries timeSeries = new WindowedTimeSeries("id", 1, Arrays.asList(window), new DumbDispatcher());
    timeSeries.publish(1, 1);
    timeSeries.publish(2, 1);
    timeSeries.close();

    Mockito.verify(consolidationListener).onConsolidation(Mockito.anyLong(), Mockito.<int[]>any());
  }

  @Test
  public void shouldConsolidationBeTriggeredWhenLastElementOfWindow2IsPublished() throws IOException {
    final ConsolidationListener consolidationListener = Mockito.mock(ConsolidationListener.class);
    final Window window = Window.of(3).listenedBy(consolidationListener).consolidatedBy(MaxConsolidator.class);
    final WindowedTimeSeries timeSeries = new WindowedTimeSeries("id", 1, Arrays.asList(window), new DumbDispatcher());
    timeSeries.publish(1, 1);
    timeSeries.publish(3, 1);
    timeSeries.close();

    Mockito.verify(consolidationListener).onConsolidation(Mockito.anyLong(), Mockito.<int[]>any());
  }

  @Test
  public void shouldConsolidationBeTriggeredWhenElementOfNewWindowIsPublished() throws IOException {
    final ConsolidationListener consolidationListener = Mockito.mock(ConsolidationListener.class);
    final Window window = Window.of(3).listenedBy(consolidationListener).consolidatedBy(MaxConsolidator.class);
    final WindowedTimeSeries timeSeries = new WindowedTimeSeries("id", 1, Arrays.asList(window), new DumbDispatcher());
    timeSeries.publish(1, 1);
    timeSeries.publish(7, 1);
    timeSeries.close();

    Mockito.verify(consolidationListener).onConsolidation(Mockito.anyLong(), Mockito.<int[]>any());
  }

  @Test
  public void shouldConsolidationBeTriggeredTwiceWhenLastElementOfNewWindowIsPublished() throws IOException {
    final ConsolidationListener consolidationListener = Mockito.mock(ConsolidationListener.class);
    final Window window = Window.of(3).listenedBy(consolidationListener).consolidatedBy(MaxConsolidator.class);
    final WindowedTimeSeries timeSeries = new WindowedTimeSeries("id", 1, Arrays.asList(window), new DumbDispatcher());
    timeSeries.publish(1, 1);
    timeSeries.publish(8, 1);
    timeSeries.close();

    Mockito.verify(consolidationListener, Mockito.times(2)).onConsolidation(Mockito.anyLong(), Mockito.<int[]>any());
  }

}