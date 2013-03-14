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

import com.github.jeluard.stone.api.Window;
import com.github.jeluard.stone.api.WindowedTimeSeries;
import com.github.jeluard.stone.consolidator.MaxConsolidator;
import com.github.jeluard.stone.dispatcher.sequential.SequentialDispatcher;
import com.github.jeluard.stone.helper.Storages;
import com.github.jeluard.stone.spi.Storage;
import com.github.jeluard.stone.storage.memory.MemoryStorage;
import com.google.common.collect.Iterables;

import java.io.IOException;
import java.util.Arrays;
import java.util.logging.Logger;

import org.junit.Assert;
import org.junit.Test;

public class WindowedTimeSeriesExample {

  @Test
  public void simpleWindowedTimeSeries() throws IOException {
    final Storage storage = new MemoryStorage(1000);
    final int size = 10;
    final Window window = Window.of(size).listenedBy(Storages.asConsolidationListener(storage, Logger.getAnonymousLogger())).consolidatedBy(MaxConsolidator.class);
    final WindowedTimeSeries windowedTimeSeries = new WindowedTimeSeries("id", 1, Arrays.asList(window), new SequentialDispatcher());
    final long now = System.currentTimeMillis();
    final int value = 1234;

    //Initiate first window => Consolidator#accumulate
    Assert.assertTrue(windowedTimeSeries.publish(now, value));

    //Last element of first window, consolidation is triggered
    Assert.assertTrue(windowedTimeSeries.publish(now + size - 1, value));

    Assert.assertEquals(now + size - 1, (long) storage.end().get());
    Assert.assertEquals(1, Iterables.size(storage.all()));
    Assert.assertEquals(value, storage.all().iterator().next().second[0]);

    //Initiate second window (even if first logical window value is missing)
    Assert.assertTrue(windowedTimeSeries.publish(now + size + 1, value+1));
 
    //Initiate third window, force consolidation of second window
    Assert.assertTrue(windowedTimeSeries.publish(now + 2*size + 2, value));
 
    Assert.assertEquals(now + 2*size - 1, (long) storage.end().get());
    Assert.assertEquals(2, Iterables.size(storage.all()));
    Assert.assertEquals(value+1, storage.all().iterator().next().second[0]);

    //Force consolidation of last (third) window
    windowedTimeSeries.close();

    Assert.assertEquals(now + 3*size - 1, (long) storage.end().get());
    Assert.assertEquals(3, Iterables.size(storage.all()));
  }

}