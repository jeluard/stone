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

import com.carrotsearch.junitbenchmarks.AbstractBenchmark;
import com.github.jeluard.stone.spi.Dispatcher;

import java.io.IOException;
import java.util.Arrays;

import org.junit.Test;
import org.mockito.Mockito;

public class TimeSeriesBenchmark extends AbstractBenchmark {

  private final TimeSeries createTimeSeries() throws IOException {
    final Listener listener = new Listener() {
      @Override
      public void onPublication(long previousTimestamp, long currentTimestamp, int value) {
      }
    };
    return new TimeSeries("id", 1, Arrays.asList(listener), Mockito.mock(Dispatcher.class));
  }

  @Test
  public void bench1MPublications() throws IOException {
    final TimeSeries timeSeries = createTimeSeries();
    for (int i = 0; i < 1000*1000; i++) {
      timeSeries.publish(i+1, i);
    }
  }

}