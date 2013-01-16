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
package com.github.jeluard.stone.impl;

import com.github.jeluard.stone.api.Archive;
import com.github.jeluard.stone.spi.Storage;
import com.github.jeluard.stone.spi.StorageFactory;
import com.google.common.base.Preconditions;

import java.io.File;
import java.io.IOException;

import journal.io.api.Journal;
import journal.io.api.RecoveryErrorHandler;

/**
 * {@link StorageFactory} implementation creating {@link JournalIOStorage}.
 */
public class JournalIOStorageFactory implements StorageFactory {

  protected Journal createJournal(final Archive archive) throws IOException {
    final Journal journal = new Journal();
    final File file = new File("stone-journal");
    file.mkdir();
    journal.setDirectory(file);
    journal.setRecoveryErrorHandler(RecoveryErrorHandler.ABORT);
    journal.setPhysicalSync(true);
    journal.open();
    return journal;
  }

  @Override
  public Storage createOrOpen(final String id, final Archive archive) throws IOException {
    Preconditions.checkNotNull(id, "null id");
    Preconditions.checkNotNull(archive, "null archive");

    return new JournalIOStorage(createJournal(archive));
  }

}