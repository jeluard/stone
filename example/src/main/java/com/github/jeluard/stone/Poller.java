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
import com.github.jeluard.stone.api.BasePoller;
import com.github.jeluard.stone.api.ConsolidationListener;
import com.github.jeluard.stone.api.Database;
import com.github.jeluard.stone.api.Window;
import com.github.jeluard.stone.impl.consolidators.Percentile95Consolidator;
import com.github.jeluard.stone.storage.journalio.JournalIOStorageFactory;
import com.google.common.base.Optional;

import java.net.URL;
import java.util.Arrays;
import java.util.Collection;

import org.joda.time.Duration;

public class Poller {
  public static void main(String[] args) throws Exception {
    final Database database = new Database(new JournalIOStorageFactory());
    final Archive archive = new Archive(Arrays.asList(Percentile95Consolidator.class), Arrays.asList(new Window(Duration.standardSeconds(10), Duration.standardDays(1))));
    final BasePoller<URL> poller = new BasePoller<URL>(database, Arrays.asList(new URL("http://google.com")), Duration.millis(100), Arrays.asList(archive)) {
      @Override
      protected String id(final URL url) {
        return url.toString();
      }
      @Override
      protected Optional<Collection<ConsolidationListener>> createConsolidationListeners(final URL t, final String id) {
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
      protected int metric(final URL url) throws Exception {
        long before = System.currentTimeMillis();
        url.openConnection().connect();
        return (int) (System.currentTimeMillis() - before);
      }
    };
    poller.start();
  }
}