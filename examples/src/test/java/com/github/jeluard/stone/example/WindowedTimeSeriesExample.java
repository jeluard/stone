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
import com.github.jeluard.stone.spi.StorageFactory;
import com.github.jeluard.stone.storage.memory.MemoryStorageFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.logging.Logger;

public class WindowedTimeSeriesExample {

  public static void main(final String[] args) throws IOException {
    final StorageFactory storageFactory = new MemoryStorageFactory();
    final Storage storage = storageFactory.createOrGet("id", 1, 1000);
    final int size = 10;
    final Window window = Window.of(size).listenedBy(Storages.asConsolidationListener(storage, Logger.getAnonymousLogger())).consolidatedBy(MaxConsolidator.class);
    final WindowedTimeSeries windowedTimeSeries = new WindowedTimeSeries("id", 1, Arrays.asList(window), new SequentialDispatcher());
    final long now = System.currentTimeMillis();
    final int value = 1234;

    //Initiate first window => Consolidator#accumulate
    windowedTimeSeries.publish(now, value);

    //Last element of first window, consolidation is triggered
    windowedTimeSeries.publish(now + size - 1, value);

    //Initiate second window (even if first logical window value is missing)
    windowedTimeSeries.publish(now + size + 1, value);
 
    //Initiate third window, force consolidation of second window
    windowedTimeSeries.publish(now + 2*size + 2, value);
 
    //Force consolidation of last (third) window
    windowedTimeSeries.close();
  }

}