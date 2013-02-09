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
package com.github.jeluard.stone.storage.journalio;

import com.github.jeluard.guayaba.test.junit.LoggerRule;
import com.github.jeluard.stone.spi.BaseStorageTest;

import java.io.IOException;

import journal.io.api.Journal;

import org.joda.time.Duration;
import org.junit.Rule;

public class JournalIOStorageTest extends BaseStorageTest<JournalIOStorage> {

  @Rule
  public LoggerRule loggerRule = new LoggerRule(JournalIOStorageFactory.LOGGER);

  @Override
  protected JournalIOStorage createStorage() throws IOException {
    final Journal journal = new Journal();
    return new JournalIOStorage(journal, Duration.standardMinutes(1));
  }

}