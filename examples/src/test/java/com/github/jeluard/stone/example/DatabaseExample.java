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

import com.github.jeluard.stone.api.TimeSeries;
import com.github.jeluard.stone.api.Window;
import com.github.jeluard.stone.consolidator.MaxConsolidator;
import com.github.jeluard.stone.dispatcher.sequential.SequentialDispatcher;
import com.github.jeluard.stone.pattern.Database;
import com.github.jeluard.stone.storage.memory.MemoryStorageFactory;

public class DatabaseExample {

  public static void main(final String[] main) throws Exception {
    final Database database = new Database(new SequentialDispatcher(), new MemoryStorageFactory());

    final TimeSeries timeSeries = database.createOrOpen("id", 1000, Window.of(10).consolidatedBy(MaxConsolidator.class));
    timeSeries.publish(System.currentTimeMillis(), 1);

    final TimeSeries timeSeries2 = database.createOrOpen("id2", 1000, Window.of(10).consolidatedBy(MaxConsolidator.class));
    timeSeries2.publish(System.currentTimeMillis(), 1);

    database.close();
  }

}