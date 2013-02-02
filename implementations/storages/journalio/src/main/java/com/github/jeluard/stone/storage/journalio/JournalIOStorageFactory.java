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

import com.github.jeluard.guayaba.lang.UncaughtExceptionHandlers;
import com.github.jeluard.guayaba.util.concurrent.ExecutorServices;
import com.github.jeluard.stone.api.Consolidator;
import com.github.jeluard.stone.api.Window;
import com.github.jeluard.stone.helper.Loggers;
import com.github.jeluard.stone.spi.BaseStorageFactory;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.Collections2;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import journal.io.api.Journal;
import journal.io.api.RecoveryErrorHandler;

import org.joda.time.Duration;

/**
 * {@link com.github.jeluard.stone.spi.StorageFactory} implementation creating {@link JournalIOStorage}.
 * <br>
 * When {@code maxFileLength} is kept low (no more than couple hundreds values) new files will be created and older ones (only containing deleted values) will be physically deleted.
 */
public class JournalIOStorageFactory extends BaseStorageFactory<JournalIOStorage> {

  static final Logger LOGGER = Loggers.create("storage.journalio");

  private static final String DEFAULT_ROOT_DIRECTORY = "stone-journal";
  private static final String WRITERS_THREAD_NAME_FORMAT = "Stone JournalIO-Writer #%d";
  private static final String DISPOSER_THREAD_NAME_FORMAT = "Stone JournalIO-Disposer";
  private static final Duration DEFAULT_COMPACTION_INTERVAL = Duration.standardMinutes(10);
  private static final String COMPACTOR_THREAD_NAME_FORMAT = "Stone JournalIO-Compactor";
  private static final String CONSOLIDATOR_SUFFIX = "Consolidator";

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
  private final ScheduledExecutorService compactionScheduler = Executors.newScheduledThreadPool(1, new ThreadFactoryBuilder().setDaemon(true).setNameFormat(JournalIOStorageFactory.COMPACTOR_THREAD_NAME_FORMAT).setUncaughtExceptionHandler(UncaughtExceptionHandlers.defaultHandler(JournalIOStorageFactory.LOGGER)).build());

  public JournalIOStorageFactory(final Executor writerExecutor, final ScheduledExecutorService disposerScheduledExecutorService) {
    this(JournalIOStorageFactory.DEFAULT_COMPACTION_INTERVAL, JournalIOStorageFactory.DEFAULT_MAX_FILE_LENGTH, writerExecutor, disposerScheduledExecutorService);
  }

  public JournalIOStorageFactory(final Duration compactionInterval, final int maxFileLength, final Executor writerExecutor, final ScheduledExecutorService disposerScheduledExecutorService) {
    this.compactionInterval = Preconditions.checkNotNull(compactionInterval, "null compactionInterval").getMillis();
    Preconditions.checkArgument(maxFileLength > 0, "maxFileLength must be > 0");
    this.maxFileLength = maxFileLength;
    this.writerExecutor = Preconditions.checkNotNull(writerExecutor, "null writerExecutor");
    this.disposerScheduledExecutorService = Preconditions.checkNotNull(disposerScheduledExecutorService, "null disposerScheduledExecutorService");
    this.compactionScheduler.scheduleWithFixedDelay(this.compactor, this.compactionInterval, this.compactionInterval, TimeUnit.MILLISECONDS);
  }

  public static Executor defaultWriteExecutor() {
    return Executors.newFixedThreadPool(2*Runtime.getRuntime().availableProcessors(), new ThreadFactoryBuilder().setDaemon(true).setNameFormat(JournalIOStorageFactory.WRITERS_THREAD_NAME_FORMAT).build());
  }

  public static ScheduledExecutorService defaultDisposerScheduledExecutor() {
    return Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder().setDaemon(true).setNameFormat(JournalIOStorageFactory.DISPOSER_THREAD_NAME_FORMAT).build());
  }

  /**
   * @param id
   * @return root directory used to store data for timeseries identified by {@code id}
   */
  protected String rootDirectoryPath(final String id) {
    return JournalIOStorageFactory.DEFAULT_ROOT_DIRECTORY+"/"+id;
  }

  /**
   * @param consolidators
   * @return all consolidators identifiers (MaxConsolidator => max)
   */
  protected final Collection<String> extractConsolidatorIdentifiers(final Collection<Class<? extends Consolidator>> consolidators) {
    return Collections2.transform(consolidators, new Function<Class<? extends Consolidator>, String>() {
      @Override
      public String apply(final Class<? extends Consolidator> input) {
        final String simpleName = input.getSimpleName();
        if (simpleName.endsWith(JournalIOStorageFactory.CONSOLIDATOR_SUFFIX)) {
          return simpleName.substring(0, simpleName.length()-JournalIOStorageFactory.CONSOLIDATOR_SUFFIX.length()).toLowerCase();
        }
        return simpleName;
      }
    });
  }

  /**
   * @param id
   * @param window
   * @param duration 
   * @return an optional prefix used when creating file names
   */
  protected Optional<String> filePrefix(final String id, final Window window, final Duration duration) {
    final Collection<String> consolidatorIdentifiers = extractConsolidatorIdentifiers(Arrays.asList(window.getConsolidatorTypes()));
    return Optional.of(Joiner.on("-").join(consolidatorIdentifiers)+"-"+duration+"@"+window.getResolution()+"-");
  }

  /**
   * @param id
   * @param window
   * @param duration 
   * @return an optional suffix used when creating file names
   */
  protected Optional<String> fileSuffix(final String id, final Window window, final Duration duration) {
    return Optional.absent();
  }

  /**
   * @param id
   * @param window
   * @param duration 
   * @return an initialized {@link Journal} dedicated to this {@code id} / {@code window} / {@code duration} tuple
   * @throws IOException 
   */
  protected Journal createJournal(final String id, final Window window, final Duration duration) throws IOException {
    final Journal journal = new Journal();
    final String mainDirectory = rootDirectoryPath(id);
    final File file = new File(mainDirectory);
    //If main directory path exists check its a directory
    //If it does not exists create it
    //Also ensures current user has write access
    if (file.exists() && !file.isDirectory()) {
      throw new IllegalArgumentException("Main directory <"+mainDirectory+"> is not a directory");
    }
    if (!file.exists() && !file.mkdirs()) {
      throw new IllegalArgumentException("Failed to create main directory <"+mainDirectory+">");
    }
    if (!file.canWrite()) {
      throw new IllegalArgumentException("Cannot write to main directory <"+mainDirectory+">");
    }
    journal.setDirectory(file);
    final Optional<String> filePrefix = filePrefix(id, window, duration);
    if (filePrefix.isPresent()) {
      journal.setFilePrefix(filePrefix.get());
    }
    final Optional<String> fileSuffix = fileSuffix(id, window, duration);
    if (fileSuffix.isPresent()) {
      journal.setFileSuffix(fileSuffix.get());
    }
    //Do not archive deleted entries
    journal.setArchiveFiles(false);
    journal.setChecksum(true);
    journal.setRecoveryErrorHandler(RecoveryErrorHandler.ABORT);
    journal.setPhysicalSync(true);
    journal.setMaxFileLength(this.maxFileLength);
    journal.setMaxWriteBatchSize(this.maxFileLength);
    journal.setWriter(this.writerExecutor);
    journal.setDisposer(this.disposerScheduledExecutorService);
    journal.open();
    return journal;
  }

  @Override
  public final JournalIOStorage create(final String id, final Window window, final Duration duration) throws IOException {
    Preconditions.checkNotNull(id, "null id");
    Preconditions.checkNotNull(window, "null window");
    Preconditions.checkNotNull(duration, "null duration");

    return new JournalIOStorage(createJournal(id, window, duration), duration);
  }

  @Override
  protected void close(final JournalIOStorage storage) throws IOException {
    storage.getJournal().close();
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