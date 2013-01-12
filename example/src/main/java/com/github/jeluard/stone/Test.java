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

/*
 * Copyright 2012 julien.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


import com.github.jeluard.stone.api.DataPoint;
import com.github.jeluard.stone.api.TimeSeries;
import com.github.jeluard.stone.impl.JournalIOStorage;
import com.github.jeluard.stone.impl.MaxConsolidator;
import com.github.jeluard.stone.impl.SequentialDispatcher;

import java.io.File;
import journal.io.api.Journal;
import journal.io.api.RecoveryErrorHandler;

import org.joda.time.DateTime;
import org.joda.time.Duration;

public class Test {
  public static void main(String[] args) throws Exception {
    final Journal journal = new Journal();
    final File file = new File("stone-journal");
    file.mkdir();
    journal.setDirectory(file);
    journal.setRecoveryErrorHandler(RecoveryErrorHandler.ABORT);
    journal.setPhysicalSync(true);
    journal.open();
    final TimeSeries timeSeries = new TimeSeries(Duration.standardMinutes(1), new SequentialDispatcher(), new JournalIOStorage(journal), new MaxConsolidator());

    while (true) {
      final long before = System.currentTimeMillis();
      for (int j = 0; j < 100000; j++) {
        Thread.sleep(1);
        timeSeries.publish(new DataPoint(DateTime.now(), (int) Math.random()));
      }
      System.out.println(System.currentTimeMillis()-before);
    }
  }
}
