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

import com.github.jeluard.guayaba.util.concurrent.ExecutorServices;
import com.github.jeluard.guayaba.util.concurrent.ThreadFactoryBuilders;
import com.github.jeluard.stone.helper.Loggers;
import com.github.jeluard.stone.spi.StorageFactory;
import com.google.common.base.Preconditions;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import journal.io.api.Journal;
import journal.io.api.RecoveryErrorHandler;

/**
 * {@link StorageFactory} implementation creating {@link JournalIOStorage}.
 * <br>
 * When {@code maxFileLength} is kept low (no more than couple hundreds values) new files will be created and older ones (only containing deleted values) will be physically deleted.
 */
public class JournalIOStorageFactory extends StorageFactory<JournalIOStorage> {

  static final Logger LOGGER = Loggers.create("storage.journalio");

  private static final String DEFAULT_ROOT_DIRECTORY = "stone-journal";
  private static final String WRITERS_THREAD_NAME_FORMAT = "Stone JournalIO-Writer #%d";
  private static final String DISPOSER_THREAD_NAME_FORMAT = "Stone JournalIO-Disposer";
  private static final long DEFAULT_COMPACTION_INTERVAL = 10*60*1000;
  private static final String COMPACTOR_THREAD_NAME_FORMAT = "Stone JournalIO-Compactor";

  //42 bytes = size of timestamp/value with 1 consolidate
  private static final int DEFAULT_MAX_FILE_LENGTH = 42*512;

  private final long compactionInterval;
  private final int maxFileLength;
  private final ScheduledExecutorService disposerScheduledExecutorService;
  private final Executor writerExecutor;
  private final Runnable compactor = new Runnable() {
    @Override
    public void run() {
      if (JournalIOStorageFactory.LOGGER.isLoggable(Level.FINEST)) {
        JournalIOStorageFactory.LOGGER.finest("About to compact");
      }

      for (final JournalIOStorage storage : getStorages()) {
        try {
          storage.compact();
        } catch (IOException e) {
          if (JournalIOStorageFactory.LOGGER.isLoggable(Level.WARNING)) {
            JournalIOStorageFactory.LOGGER.log(Level.WARNING, "Got exception while compacting <"+storage+">", e);
          }
        }
      }

      if (JournalIOStorageFactory.LOGGER.isLoggable(Level.FINEST)) {
        JournalIOStorageFactory.LOGGER.finest("Compaction done");
      }
    }
  };
  private final ScheduledExecutorService compactionScheduler = Executors.newScheduledThreadPool(1, ThreadFactoryBuilders.safeBuilder(JournalIOStorageFactory.COMPACTOR_THREAD_NAME_FORMAT, JournalIOStorageFactory.LOGGER).build());

  public JournalIOStorageFactory(final Executor writerExecutor, final ScheduledExecutorService disposerScheduledExecutorService) {
    this(JournalIOStorageFactory.DEFAULT_COMPACTION_INTERVAL, JournalIOStorageFactory.DEFAULT_MAX_FILE_LENGTH, writerExecutor, disposerScheduledExecutorService);
  }

  public JournalIOStorageFactory(final long compactionInterval, final int maxFileLength, final Executor writerExecutor, final ScheduledExecutorService disposerScheduledExecutorService) {
    this.compactionInterval = Preconditions.checkNotNull(compactionInterval, "null compactionInterval");
    Preconditions.checkArgument(maxFileLength > 0, "maxFileLength must be > 0");
    this.maxFileLength = maxFileLength;
    this.writerExecutor = Preconditions.checkNotNull(writerExecutor, "null writerExecutor");
    this.disposerScheduledExecutorService = Preconditions.checkNotNull(disposerScheduledExecutorService, "null disposerScheduledExecutorService");
    this.compactionScheduler.scheduleWithFixedDelay(this.compactor, this.compactionInterval, this.compactionInterval, TimeUnit.MILLISECONDS);
  }

  public static Executor defaultWriteExecutor() {
    return Executors.newFixedThreadPool(2*Runtime.getRuntime().availableProcessors(), ThreadFactoryBuilders.safeBuilder(JournalIOStorageFactory.WRITERS_THREAD_NAME_FORMAT, JournalIOStorageFactory.LOGGER).build());
  }

  public static ScheduledExecutorService defaultDisposerScheduledExecutor() {
    return Executors.newSingleThreadScheduledExecutor(ThreadFactoryBuilders.safeBuilder(JournalIOStorageFactory.DISPOSER_THREAD_NAME_FORMAT, JournalIOStorageFactory.LOGGER).build());
  }

  /**
   * @param id
   * @return root directory used to store data for timeseries identified by {@code id}
   */
  protected String rootDirectoryPath(final String id) {
    return JournalIOStorageFactory.DEFAULT_ROOT_DIRECTORY+"/"+id;
  }

  /**
   * @param id
   * @return a prefix used when creating file names
   */
  protected String filePrefix(final String id) {
    return id+"-";
  }

  /**
   * @param id
   * @return an initialized {@link Journal} dedicated to this {@code id}
   * @throws IOException 
   */
  protected Journal createJournal(final String id) throws IOException {
    final String mainDirectory = rootDirectoryPath(id);
    final File file = new File(mainDirectory);
    //If it does not exists create it
    if (!file.exists() && !file.mkdirs()) {
      throw new IllegalArgumentException("Failed to create main directory <"+mainDirectory+">");
    }
    final ExtendedJournal.Builder builder = ExtendedJournal.of(file);
    final String filePrefix = filePrefix(id);
    builder.setFilePrefix(filePrefix);
    builder.setChecksum(true);
    builder.setRecoveryErrorHandler(RecoveryErrorHandler.ABORT);
    builder.setPhysicalSync(true);
    builder.setMaxFileLength(this.maxFileLength).setMaxWriteBatchSize(this.maxFileLength);
    builder.setWriter(this.writerExecutor).setDisposer(this.disposerScheduledExecutorService);
    return builder.open();
  }

  @Override
  public JournalIOStorage create(final String id, final int maximumSize) throws IOException {
    return new JournalIOStorage(maximumSize, createJournal(id), JournalIOStorage.DEFAULT_WRITE_CALLBACK);
  }

  private void delete(final File directory) {
    final File[] files = directory.listFiles();
    if (files!=null) {
      for (final File file: files) {
        if(file.isDirectory()) {
          delete(file);
        } else {
          file.delete();
        }
      }
    }
    directory.delete();
  }

  /**
   * Note that this implementation does not assume any layout for {@link #rootDirectoryPath(java.lang.String)} as this might be dangerous.
   * Thus only {@link #rootDirectoryPath(java.lang.String)} will be deleted (and no other parent directory).
   *
   * @param id
   * @throws IOException 
   */
  @Override
  public void delete(final String id) throws IOException {
    Preconditions.checkNotNull(id, "null id");

    final String mainDirectory = rootDirectoryPath(id);
    final File directory = new File(mainDirectory);
    if (directory.isDirectory()) {
      delete(directory);
    }
  }

  @Override
  protected void cleanup() {
    ExecutorServices.shutdownAndAwaitTermination(this.compactionScheduler, this.compactionInterval, TimeUnit.MILLISECONDS, JournalIOStorageFactory.LOGGER);
  }

}