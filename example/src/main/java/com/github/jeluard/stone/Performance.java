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

import com.github.jeluard.guayaba.base.Pair;
import com.github.jeluard.stone.api.Archive;
import com.github.jeluard.stone.api.Window;
import com.github.jeluard.stone.api.TimeSeries;
import com.github.jeluard.stone.impl.JournalIOStorageFactory;
import com.github.jeluard.stone.impl.consolidators.MaxConsolidator;
import com.github.jeluard.stone.impl.SequentialDispatcher;
import com.github.jeluard.stone.impl.consolidators.MeanConsolidator;
import com.github.jeluard.stone.impl.consolidators.Percentile999Consolidator;
import com.github.jeluard.stone.spi.Storage;
import com.github.jeluard.stone.spi.StorageFactory;
import java.util.ArrayList;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.joda.time.Duration;

public class Performance {
  public static void main(String[] args) throws Exception {
    final Archive archive1 = new Archive(Arrays.asList(MaxConsolidator.class), 
            Arrays.asList(new Window(Duration.standardMinutes(1), Duration.standardHours(1))));
    final Archive archive2 = new Archive(Arrays.asList(MaxConsolidator.class, MaxConsolidator.class), 
            Arrays.asList(new Window(Duration.standardMinutes(5), Duration.standardHours(2))));

    int nbSeries = 5000;

    final List<TimeSeries> timeSeries = new ArrayList<TimeSeries>(nbSeries);
    final StorageFactory factory = new JournalIOStorageFactory();
    for (int i = 0; i < nbSeries; i++) {
      timeSeries.add(new TimeSeries("ping-server-"+i, Arrays.asList(archive1, archive2), new SequentialDispatcher(), factory));
    }

    /*for (final TimeSeries ts : timeSeries) {
      final Map<Pair<Archive, Window>, Storage> storages = ts.getStorages();
      System.out.println("TimeSeries "+ts.getId());
      for (final Map.Entry<Pair<Archive, Window>, Storage> entry : storages.entrySet()) {
        System.out.println("\tfor sampling "+entry.getKey().second);
        for (final Pair<Long, int[]> value : entry.getValue().all()) {
          System.out.println("\t\ttimestamp <"+value.first+"> values <"+Arrays.toString(value.second)+">");
        }
        
      }
      System.out.println();
    }*/

   // System.exit(-1);

    try {
      final Random random = new Random();
      for (int i = 0; i < 1000; i++) {
        final long before = System.currentTimeMillis();
        for (final TimeSeries ts : timeSeries) {
          ts.publish(System.currentTimeMillis(), 100+random.nextInt(25));
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
