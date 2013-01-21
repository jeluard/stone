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
package com.github.jeluard.stone;

import com.github.jeluard.stone.api.Archive;
import com.github.jeluard.stone.api.TimeSeries;
import com.github.jeluard.stone.api.Window;
import com.github.jeluard.stone.impl.JournalIOStorageFactory;
import com.github.jeluard.stone.impl.consolidators.MaxConsolidator;
import com.github.jeluard.stone.spi.StorageFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import org.joda.time.Duration;

public class Performance {
  public static void main(String[] args) throws Exception {
    final Archive archive1 = new Archive(Arrays.asList(MaxConsolidator.class), 
            Arrays.asList(new Window(Duration.standardMinutes(5), Duration.standardDays(1))));
    final Archive archive2 = new Archive(Arrays.asList(MaxConsolidator.class, MaxConsolidator.class), 
            Arrays.asList(new Window(Duration.standardHours(1), Duration.standardDays(7))));
    final Archive archive3 = new Archive(Arrays.asList(MaxConsolidator.class, MaxConsolidator.class), 
            Arrays.asList(new Window(Duration.standardDays(1), Duration.standardDays(365))));

    final int nbSeries = 10000;

    final List<TimeSeries> timeSeries = new ArrayList<TimeSeries>(nbSeries);
    final StorageFactory factory = new JournalIOStorageFactory();
    for (int i = 0; i < nbSeries; i++) {
      timeSeries.add(new TimeSeries("ping-server-"+i, Arrays.asList(archive1, archive2, archive3), factory));
    }

    try {
      final Random random = new Random();
      for (int i = 0; i < 100000; i++) {
        final long before = System.currentTimeMillis();
        for (int j = 0; j < 45*100; j++) {
          for (final TimeSeries ts : timeSeries) {
            ts.publish(System.currentTimeMillis(), 100);
          }
        }
        System.out.println(System.currentTimeMillis()-before);
      }
    } finally {
      for (final TimeSeries ts : timeSeries) {
        ts.close();
      }
    }
  }
}
