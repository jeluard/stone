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
package com.github.jeluard.stone.example;

import com.github.jeluard.stone.api.Listener;
import com.github.jeluard.stone.api.TimeSeries;
import com.github.jeluard.stone.dispatcher.sequential.SequentialDispatcher;

import java.util.Arrays;

public class TimeSeriesExample {

  public static void main(final String[] main) {
    final TimeSeries timeSeries = new TimeSeries("id", 2, Arrays.asList(new Listener() {
      @Override
      public void onPublication(long previousTimestamp, long currentTimestamp, int value) {
        System.out.println("Received value <"+value+"> for timestamp <"+currentTimestamp+">");
      }
    }), new SequentialDispatcher());

    final long now = System.currentTimeMillis();
    timeSeries.publish(now, 1);//return true, trigger listener
    timeSeries.publish(now+1, 1);//return false as do not match provided granularity
    timeSeries.publish(now+2, 1);//return true, trigger listener
    timeSeries.close();
  }

}