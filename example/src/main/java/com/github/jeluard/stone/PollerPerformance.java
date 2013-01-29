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
import com.github.jeluard.stone.api.BasePoller;
import com.github.jeluard.stone.api.ConsolidationListener;
import com.github.jeluard.stone.api.Database;
import com.github.jeluard.stone.api.Window;
import com.github.jeluard.stone.consolidator.Percentile95Consolidator;
import com.github.jeluard.stone.dispatcher.sequential.SequentialDispatcher;
import com.github.jeluard.stone.storage.journalio.JournalIOStorageFactory;
import com.google.common.base.Optional;

import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.Random;

import org.joda.time.Duration;

public class PollerPerformance {
  public static void main(String[] args) throws Exception {
    final int sampleSize = 1000;
    final Random random = new Random();
    final Collection<Pair<Integer, Integer>> ints = new LinkedList<Pair<Integer, Integer>>();
    for (int i = 0; i < sampleSize; i++) {
      ints.add(new Pair<Integer, Integer>(i, 100+random.nextInt(20)));
    }

    final Database database = new Database(new SequentialDispatcher(), new JournalIOStorageFactory(JournalIOStorageFactory.defaultWriteExecutor(), JournalIOStorageFactory.defaultDisposerScheduledExecutor()));
    final Archive archive = new Archive(Arrays.asList(Percentile95Consolidator.class), Arrays.asList(new Window(Duration.standardSeconds(10), Duration.standardDays(1))));
    final BasePoller<Pair<Integer, Integer>> poller = new BasePoller<Pair<Integer, Integer>>(database, ints, Duration.millis(1000), Arrays.asList(archive)) {
      @Override
      protected String id(final Pair<Integer, Integer> pair) {
        return pair.first.toString();
      }
      @Override
      protected Optional<Collection<ConsolidationListener>> createConsolidationListeners(final Pair<Integer, Integer> pair, final String id) {
        final ConsolidationListener consolidationListener = new ConsolidationListener() {
          @Override
          public void onConsolidation(final Window window, final long timestamp, final int[] consolidates) {
            System.out.println("Got for <"+id+"> "+Arrays.toString(consolidates));
          }
        };
        final Collection<ConsolidationListener> consolidationListeners = Arrays.asList(consolidationListener);
        return Optional.of(consolidationListeners);
      }
      @Override
      protected int metric(final Pair<Integer, Integer> pair) throws Exception {
        long before = System.currentTimeMillis();
        Thread.sleep(pair.second);
        return (int) (System.currentTimeMillis() - before);
      }
    };
    poller.start();
    Thread.currentThread().join();
  }
}