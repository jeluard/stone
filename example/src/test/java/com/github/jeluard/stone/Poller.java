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
import com.github.jeluard.stone.api.Window;
import com.github.jeluard.stone.consolidator.Percentile95Consolidator;
import com.github.jeluard.stone.dispatcher.sequential.SequentialDispatcher;
import com.github.jeluard.stone.pattern.BasePoller;
import com.github.jeluard.stone.storage.journalio.JournalIOStorageFactory;
import com.google.common.util.concurrent.Futures;

import java.net.URL;
import java.util.Arrays;
import java.util.concurrent.Future;

import org.joda.time.Duration;

public class Poller {
  public static void main(String[] args) throws Exception {
    final Database database = new Database(new SequentialDispatcher(), new JournalIOStorageFactory(JournalIOStorageFactory.defaultWriteExecutor(), JournalIOStorageFactory.defaultDisposerScheduledExecutor()));
    final Window window = Window.of(Duration.standardSeconds(10)).persistedDuring(Duration.standardDays(1)).consolidatedBy(Percentile95Consolidator.class);
    final BasePoller<URL> poller = new BasePoller<URL>(database, Arrays.asList(new URL("http://google.com")), Duration.millis(1000), Arrays.asList(window)) {
      @Override
      protected String id(final URL url) {
        return url.toString();
      }
      @Override
      protected Future<Integer> metric(final URL url) {
        long before = System.currentTimeMillis();
      //  url.openConnection().connect();
        return Futures.immediateFuture((int)(System.currentTimeMillis() - before));
      }
    };
    poller.start();
    Thread.currentThread().join();
  }
}