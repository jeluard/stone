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
import com.github.jeluard.stone.api.ConsolidationListener;
import com.github.jeluard.stone.api.Database;
import com.github.jeluard.stone.api.TimeSeries;
import com.github.jeluard.stone.api.Window;
import com.github.jeluard.stone.consolidator.MaxConsolidator;
import com.github.jeluard.stone.consolidator.MinConsolidator;
import com.github.jeluard.stone.consolidator.LastConsolidator;
import com.github.jeluard.stone.consolidator.Percentile90Consolidator;
import com.github.jeluard.stone.dispatcher.blocking_queue.BlockingQueueDispatcher;
import com.github.jeluard.stone.dispatcher.sequential.SequentialDispatcher;
import com.github.jeluard.stone.storage.journalio.JournalIOStorageFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;

import org.joda.time.Duration;

public class Performance {
  public static void main(String[] args) throws Exception {
    final Database database = new Database(new BlockingQueueDispatcher(new ArrayBlockingQueue<BlockingQueueDispatcher.Entry>(1000000), BlockingQueueDispatcher.defaultExecutorService(), 2), new JournalIOStorageFactory(JournalIOStorageFactory.defaultWriteExecutor(), JournalIOStorageFactory.defaultDisposerScheduledExecutor()));
    final Archive archive1 = new Archive(Arrays.asList(MaxConsolidator.class), 
            Arrays.asList(new Window(Duration.standardMinutes(5), Duration.standardDays(1)),
                          new Window(Duration.standardHours(1), Duration.standardDays(7)),
                          new Window(Duration.standardDays(1), Duration.standardDays(365))));
    final Archive archive2 = new Archive(Arrays.asList(/*Percentile90Consolidator.class, */MaxConsolidator.class), 
            Arrays.asList(new Window(Duration.standardMinutes(1), Duration.standardDays(7))));

    final int nbSeries = 1;

    final List<TimeSeries> timeSeries = new ArrayList<TimeSeries>(nbSeries);
    for (int i = 0; i < nbSeries; i++) {
      timeSeries.add(database.createOrOpen("ping-server-"+i, Arrays.asList(/*archive1, */archive2)));
    }
    final TimeSeries ts = database.createOrOpen("ping-server", Arrays.asList(/*archive1, */archive2));

    try {
      long timestamp = 1;
      for (int i = 0; i < 100000; i++) {
        final long before = System.currentTimeMillis();
        for (int j = 0; j < 1000000; j++) {
          //Increment timestamp once each iteration
          timestamp++;
          //for (final TimeSeries ts : timeSeries) {
            ts.publish(timestamp, 100);
          //}
        }
        System.out.println((System.currentTimeMillis()-before)+" ms");
      }
    } finally {
      database.close();
    }
  }
}
