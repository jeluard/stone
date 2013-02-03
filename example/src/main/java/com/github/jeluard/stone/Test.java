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
import com.github.jeluard.stone.api.ConsolidationListener;
import com.github.jeluard.stone.api.Database;
import com.github.jeluard.stone.api.Reader;
import com.github.jeluard.stone.api.TimeSeries;
import com.github.jeluard.stone.api.Window;
import com.github.jeluard.stone.consolidator.MaxConsolidator;
import com.github.jeluard.stone.dispatcher.disruptor.DisruptorDispatcher;
import com.github.jeluard.stone.storage.journalio.JournalIOStorageFactory;

import java.util.Arrays;
import java.util.Map;
import java.util.Random;

import org.joda.time.Duration;

public class Test {
  public static void main(String[] args) throws Exception {
    final Database database = new Database(new DisruptorDispatcher(DisruptorDispatcher.defaultExecutorService(), 1024), new JournalIOStorageFactory(JournalIOStorageFactory.defaultWriteExecutor(), JournalIOStorageFactory.defaultDisposerScheduledExecutor()));
    final ConsolidationListener consolidationListener = new ConsolidationListener() {
      @Override
      public void onConsolidation(final long timestamp, final int[] consolidates) {
        System.out.println("Got "+Arrays.toString(consolidates));
      }
    };
    final Window window = Window.of(Duration.standardSeconds(10)).listenedBy(consolidationListener).archivedDuring(Duration.standardMinutes(1)).consolidatedBy(MaxConsolidator.class);
    final TimeSeries timeSeries = database.createOrOpen("timeseries", window);
    final Map<Window, ? extends Reader> readers = timeSeries.getReaders();
    System.out.println("TimeSeries "+timeSeries.getId());
    for (final Reader entry : readers.values()) {
      for (final Pair<Long, int[]> value : entry.all()) {
        System.out.println("\t\ttimestamp <"+value.first+"> values <"+Arrays.toString(value.second)+">");
      }
    }

    try {
      final Random random = new Random();
      for (int i = 0; i < 1000*1000; i++) {
        //Thread.sleep(1);//Make sure there's at least 1ms in between publication
        timeSeries.publish(System.currentTimeMillis(), 100+random.nextInt(25));
      }
    } finally {
      database.close();
    }
  }
}
