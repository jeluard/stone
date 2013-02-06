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

import com.github.jeluard.stone.api.Database;
import com.github.jeluard.stone.api.TimeSeries;
import com.github.jeluard.stone.api.Window;
import com.github.jeluard.stone.consolidator.MaxConsolidator;
import com.github.jeluard.stone.dispatcher.blocking_queue.BlockingQueueDispatcher;
import com.github.jeluard.stone.storage.journalio.JournalIOStorageFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;

import org.joda.time.Duration;

public class Performance {
  public static void main(String[] args) throws Exception {
    final Database database = new Database(new BlockingQueueDispatcher(new ArrayBlockingQueue<BlockingQueueDispatcher.Entry>(1000000), BlockingQueueDispatcher.defaultExecutorService(), 2), new JournalIOStorageFactory(JournalIOStorageFactory.defaultWriteExecutor(), JournalIOStorageFactory.defaultDisposerScheduledExecutor()));
    final Window window1 = Window.of(Duration.standardMinutes(5)).persistedDuring(Duration.standardDays(1)).consolidatedBy(MaxConsolidator.class);
    final Window window2 = Window.of(Duration.standardMinutes(1)).persistedDuring(Duration.standardDays(7)).consolidatedBy(MaxConsolidator.class);

    final int nbSeries = 1;

    final List<TimeSeries> timeSeries = new ArrayList<TimeSeries>(nbSeries);
    for (int i = 0; i < nbSeries; i++) {
      timeSeries.add(database.createOrOpen("ping-server-"+i, /*window1, */window2));
    }
    final TimeSeries ts = database.createOrOpen("ping-server", /*window1, */window2);

    try {
      long timestamp = 1;
      for (int i = 0; i < 100000; i++) {
        final long before = System.currentTimeMillis();
        for (int j = 0; j < 1000*1000; j++) {
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