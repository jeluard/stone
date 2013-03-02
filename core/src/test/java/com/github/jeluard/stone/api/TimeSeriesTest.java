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

import com.github.jeluard.guayaba.test.junit.LoggerRule;
import com.github.jeluard.stone.helper.Loggers;
import com.github.jeluard.stone.spi.Dispatcher;

import java.io.IOException;
import java.util.Arrays;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

public class TimeSeriesTest {

  @Rule
  public final LoggerRule loggerRule = new LoggerRule(Loggers.BASE_LOGGER);

  private Listener createListener() {
    return new Listener() {
      @Override
      public void onPublication(long previousTimestamp, long currentTimestamp, int value) {
      }
    };
  }

  @Test
  public void shouldCreationBeSuccessful() throws IOException {
    final TimeSeries timeSeries = new TimeSeries("id", 1, Arrays.asList(createListener()), Mockito.mock(Dispatcher.class));
    timeSeries.close();
  }

  @Test
  public void shouldIDBeAccessible() throws IOException {
    final String id = "id";
    final TimeSeries timeSeries = new TimeSeries(id, 1, Arrays.asList(createListener()), Mockito.mock(Dispatcher.class));
    timeSeries.close();

    Assert.assertEquals(id, timeSeries.getId());
  }

  @Test
  public void shouldCloseBeIdempotent() throws IOException {
    final TimeSeries timeSeries = new TimeSeries("id", 1, Arrays.asList(createListener()), Mockito.mock(Dispatcher.class));
    timeSeries.close();
    timeSeries.close();
  }

  @Test(expected=IllegalArgumentException.class)
  public void shouldCreationWithDuplicatedIDBeInvalid() throws IOException {
    final String id = "id";
    final TimeSeries timeSeries = new TimeSeries(id, 1, Arrays.asList(createListener()), Mockito.mock(Dispatcher.class));
    try {
      new TimeSeries(id, 1, Arrays.asList(createListener()), Mockito.mock(Dispatcher.class));
    } finally {
      timeSeries.close();
    }
  }

  @Test
  public void shouldCreationWithDuplicatedIDBeSuccessfulAfterClose() throws IOException {
    final String id = "id";
    final TimeSeries timeSeries = new TimeSeries(id, 1, Arrays.asList(createListener()), Mockito.mock(Dispatcher.class));
    timeSeries.close();
    final TimeSeries timeSeries2 = new TimeSeries(id, 1, Arrays.asList(createListener()), Mockito.mock(Dispatcher.class));
    timeSeries2.close(); 
  }

  @Test
  public void shouldPublicationBeSuccessful() throws IOException {
    final TimeSeries timeSeries = new TimeSeries("id", 1, Arrays.asList(createListener()), Mockito.mock(Dispatcher.class));
    timeSeries.publish(1, 1);
    timeSeries.close();
  }

  @Test
  public void shouldMonotonicPublicationBeSuccessful() throws IOException {
    final TimeSeries timeSeries = new TimeSeries("id", 1, Arrays.asList(createListener()), Mockito.mock(Dispatcher.class));
    timeSeries.publish(1, 1);
    timeSeries.publish(2, 1);
    timeSeries.close();
  }

  @Test
  public void shouldZeroAsTimestampBeRejected() throws IOException {
    final TimeSeries timeSeries = new TimeSeries("id", 1, Arrays.asList(createListener()), Mockito.mock(Dispatcher.class));
    Assert.assertFalse(timeSeries.publish(0, 0));
    timeSeries.close();
  }

  @Test
  public void shouldTwiceSameTimestampBeRejected() throws IOException {
    final TimeSeries timeSeries = new TimeSeries("id", 1, Arrays.asList(createListener()), Mockito.mock(Dispatcher.class));
    final long now = System.currentTimeMillis();
    Assert.assertTrue(timeSeries.publish(now, 0));
    Assert.assertFalse(timeSeries.publish(now, 0));
    timeSeries.close();
  }

  @Test
  public void shouldTimestampCompatibleWithGranularityBeAccepted() throws IOException {
    final TimeSeries timeSeries = new TimeSeries("id", 1, Arrays.asList(createListener()), Mockito.mock(Dispatcher.class));
    final long now = System.currentTimeMillis();
    Assert.assertTrue(timeSeries.publish(now, 0));
    Assert.assertTrue(timeSeries.publish(now+1, 0));
    Assert.assertTrue(timeSeries.publish(now+2, 0));
    timeSeries.close();
  }

  @Test
  public void shouldTimestampIncompatibleWithGranularityBeRejected() throws IOException {
    final TimeSeries timeSeries = new TimeSeries("id", 2, Arrays.asList(createListener()), Mockito.mock(Dispatcher.class));
    final long now = System.currentTimeMillis();
    Assert.assertTrue(timeSeries.publish(now, 0));
    Assert.assertFalse(timeSeries.publish(now+1, 0));
    Assert.assertTrue(timeSeries.publish(now+2, 0));
    timeSeries.close();
  }

}