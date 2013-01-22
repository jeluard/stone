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
import com.github.jeluard.stone.api.Archive;
import com.github.jeluard.stone.api.Consolidator;
import com.github.jeluard.stone.api.Window;
import com.github.jeluard.stone.helper.Loggers;
import com.github.jeluard.stone.spi.BaseStorageFactory;
import com.github.jeluard.stone.spi.StorageFactory;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.Collections2;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.io.File;
import java.io.IOException;
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
 * {@link StorageFactory} implementation creating {@link JournalIOStorage}.
 * <br>
 * When {@code maxFileLength} is kept low (no more than couple hundreds values) new files will be created and older ones (only containing deleted values) will be physically deleted.
 */
public class JournalIOStorageFactory extends BaseStorageFactory<JournalIOStorage> {

  static final Logger LOGGER = Loggers.create("storage.journalio");

  private static final String WRITER_THREADS_NAME_FORMAT = "Stone JournalIO-Writer #%d";
  private static final String DISPOSER_THREADS_NAME_FORMAT = "Stone JournalIO-Disposer";
  private static final Duration DEFAULT_COMPACTION_INTERVAL = Duration.standardMinutes(10);
  private static final String COMPACTOR_THREAD = "Stone JournalIO-Compactor";
  private static final String CONSOLIDATOR_SUFFIX = "Consolidator";

  private static final int MAX_FILE_LENGTH = 42*512;//42 bytes (size of timestamp/value with 1 consolidate)

  private final long compactionInterval;
  private final ScheduledExecutorService disposerScheduledExecutorService;
  private final Executor writerExecutor;
  private final Runnable compactor = new Runnable() {
    @Override
    public void run() {
      try {
        if (JournalIOStorageFactory.LOGGER.isLoggable(Level.FINEST)) {
          JournalIOStorageFactory.LOGGER.finest("About to compact");
        }

        for (final JournalIOStorage storage : getStorages()) {
          storage.compact();
        }
      } catch (IOException e) {
        if (JournalIOStorageFactory.LOGGER.isLoggable(Level.WARNING)) {
          JournalIOStorageFactory.LOGGER.log(Level.WARNING, "Got exception while compacting", e);
        }
      }
    }
  };
  private final ScheduledExecutorService compactionScheduler = Executors.newScheduledThreadPool(1, new ThreadFactoryBuilder().setDaemon(true).setNameFormat(JournalIOStorageFactory.COMPACTOR_THREAD).build());

  public JournalIOStorageFactory() {
    this(JournalIOStorageFactory.DEFAULT_COMPACTION_INTERVAL, JournalIOStorageFactory.defaultWriteExecutor(), JournalIOStorageFactory.defaultDisposerScheduledExecutor());
  }

  public JournalIOStorageFactory(final Duration compactionInterval, final Executor writerExecutor, final ScheduledExecutorService disposerScheduledExecutorService) {
    this.compactionInterval = compactionInterval.getMillis();
    this.writerExecutor = Preconditions.checkNotNull(writerExecutor, "null writerExecutor");
    this.disposerScheduledExecutorService = Preconditions.checkNotNull(disposerScheduledExecutorService, "null disposerScheduledExecutorService");
    this.compactionScheduler.scheduleWithFixedDelay(this.compactor, this.compactionInterval, this.compactionInterval, TimeUnit.MILLISECONDS);
  }

  public static Executor defaultWriteExecutor() {
    return Executors.newFixedThreadPool(2*Runtime.getRuntime().availableProcessors(), new ThreadFactoryBuilder().setDaemon(true).setNameFormat(JournalIOStorageFactory.WRITER_THREADS_NAME_FORMAT).build());
  }

  public static ScheduledExecutorService defaultDisposerScheduledExecutor() {
    return Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder().setDaemon(true).setNameFormat(JournalIOStorageFactory.DISPOSER_THREADS_NAME_FORMAT).build());
  }

  protected String mainDirectoryPath(final String id, final Archive archive, final Window window) {
    return "stone-journal/"+id;
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
   * @param archive
   * @return an optional prefix used when creating file names
   */
  protected Optional<String> filePrefix(final String id, final Archive archive, final Window window) {
    final Collection<String> consolidatorIdentifiers = extractConsolidatorIdentifiers(archive.getConsolidators());
    return Optional.of(Joiner.on("-").join(consolidatorIdentifiers)+"-"+window.getDuration()+"@"+window.getResolution()+"-");
  }

  /**
   * @param id
   * @param archive
   * @return an optional suffix used when creating file names
   */
  protected Optional<String> fileSuffix(final String id, final Archive archive, final Window window) {
    return Optional.absent();
  }

  /**
   * @param id
   * @param archive
   * @param window 
   * @return an initialized {@link Journal} dedicated to this {@code id} / {@code archive} / {@code window} tuple
   * @throws IOException 
   */
  protected Journal createJournal(final String id, final Archive archive, final Window window) throws IOException {
    final Journal journal = new Journal();
    final String mainDirectory = mainDirectoryPath(id, archive, window);
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
    final Optional<String> filePrefix = filePrefix(id, archive, window);
    if (filePrefix.isPresent()) {
      journal.setFilePrefix(filePrefix.get());
    }
    final Optional<String> fileSuffix = fileSuffix(id, archive, window);
    if (fileSuffix.isPresent()) {
      journal.setFileSuffix(fileSuffix.get());
    }
    //Do not archive deleted entries
    journal.setArchiveFiles(false);
    journal.setChecksum(true);
    journal.setRecoveryErrorHandler(RecoveryErrorHandler.ABORT);
    journal.setPhysicalSync(true);
    journal.setMaxFileLength(JournalIOStorageFactory.MAX_FILE_LENGTH);
    journal.setMaxWriteBatchSize(JournalIOStorageFactory.MAX_FILE_LENGTH);
    journal.setWriter(this.writerExecutor);
    journal.setDisposer(this.disposerScheduledExecutorService);
    journal.open();
    return journal;
  }

  @Override
  public final JournalIOStorage create(final String id, final Archive archive, final Window window) throws IOException {
    Preconditions.checkNotNull(id, "null id");
    Preconditions.checkNotNull(archive, "null archive");
    Preconditions.checkNotNull(window, "null window");

    return new JournalIOStorage(createJournal(id, archive, window), window.getDuration());
  }

  @Override
  protected void cleanup(final JournalIOStorage storage) throws IOException {
    storage.close();
  }

  @Override
  protected void cleanup() {
    ExecutorServices.shutdownAndAwaitTermination(this.compactionScheduler, this.compactionInterval, TimeUnit.MILLISECONDS, JournalIOStorageFactory.LOGGER);
  }

}