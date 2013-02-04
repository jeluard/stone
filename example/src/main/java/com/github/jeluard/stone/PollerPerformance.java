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
import com.github.jeluard.stone.api.BasePoller;
import com.github.jeluard.stone.api.Database;
import com.github.jeluard.stone.api.Window;
import com.github.jeluard.stone.consolidator.Percentile95Consolidator;
import com.github.jeluard.stone.dispatcher.sequential.SequentialDispatcher;
import com.github.jeluard.stone.storage.journalio.JournalIOStorageFactory;
import com.google.common.util.concurrent.AbstractFuture;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Future;

import org.joda.time.Duration;

public class PollerPerformance {
  public static void main(String[] args) throws Exception {
    class DummyFuture extends AbstractFuture<Integer> {
      public void done() {
        set(100);
      }
    }
    class Triggerer implements Runnable {

      private final List<DummyFuture> futures = new CopyOnWriteArrayList<DummyFuture>();

      @Override
      public void run() {
        while (!Thread.currentThread().isInterrupted()) {
          try {
            Thread.sleep(100);
          } catch (InterruptedException ex) {
          }

          for (final Iterator<DummyFuture> it = this.futures.iterator(); it.hasNext();) {
            final DummyFuture abstractFuture = it.next();
            abstractFuture.done();
            this.futures.remove(abstractFuture);
          }
        }
      }
      private void enqueue(final DummyFuture abstractFuture) {
        this.futures.add(abstractFuture);
      }
    }
            
    final int sampleSize = 1000;
    final Random random = new Random();
    final Collection<Pair<Integer, Integer>> ints = new LinkedList<Pair<Integer, Integer>>();
    for (int i = 0; i < sampleSize; i++) {
      ints.add(new Pair<Integer, Integer>(i, 100+random.nextInt(20)));
    }

    final Database database = new Database(new SequentialDispatcher(), new JournalIOStorageFactory(JournalIOStorageFactory.defaultWriteExecutor(), JournalIOStorageFactory.defaultDisposerScheduledExecutor()));
    final Window window = Window.of(Duration.standardSeconds(10)).consolidatedBy(Percentile95Consolidator.class);
    final Triggerer triggerer = new Triggerer();
    final BasePoller<Pair<Integer, Integer>> poller = new BasePoller<Pair<Integer, Integer>>(database, ints, Duration.millis(1000), Arrays.asList(window)) {
      @Override
      protected String id(final Pair<Integer, Integer> pair) {
        return pair.first.toString();
      }
      @Override
      protected Future<Integer> metric(final Pair<Integer, Integer> pair) {
        final DummyFuture abstractFuture=  new DummyFuture();
        triggerer.enqueue(abstractFuture);
        return abstractFuture;
      }
    };
    Thread thread = new Thread(triggerer, "Triggerer");
    thread.start();
    poller.start();
    thread.join();
  }
}