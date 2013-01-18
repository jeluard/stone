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
import com.github.jeluard.stone.api.SamplingWindow;
import com.github.jeluard.stone.spi.Consolidator;
import com.github.jeluard.stone.spi.Storage;
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

import journal.io.api.Journal;
import journal.io.api.RecoveryErrorHandler;

/**
 * {@link StorageFactory} implementation creating {@link JournalIOStorage}.
 */
public class JournalIOStorageFactory implements StorageFactory {

  private static final String WRITER_THREADS_NAME_FORMAT = "Stone JournalIO-Writer #%d";
  private static final String DISPOSER_THREADS_NAME_FORMAT = "Stone JournalIO-Disposer";
  private static final String CONSOLIDATOR_SUFFIX = "Consolidator";

  private static final int MAX_FILE_LENGTH = 1024 * 1024;//1MB

  private final ScheduledExecutorService disposerScheduledExecutorService;
  private final Executor writerExecutor;

  public JournalIOStorageFactory() {
    this(Executors.newFixedThreadPool(2*Runtime.getRuntime().availableProcessors(), new ThreadFactoryBuilder().setDaemon(true).setNameFormat(JournalIOStorageFactory.WRITER_THREADS_NAME_FORMAT).build()),
         Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder().setDaemon(true).setNameFormat(JournalIOStorageFactory.DISPOSER_THREADS_NAME_FORMAT).build()));
  }

  public JournalIOStorageFactory(final Executor writerExecutor, final ScheduledExecutorService disposerScheduledExecutorService) {
    this.writerExecutor = Preconditions.checkNotNull(writerExecutor, "null writerExecutor");
    this.disposerScheduledExecutorService = Preconditions.checkNotNull(disposerScheduledExecutorService, "null disposerScheduledExecutorService");
  }

  protected String mainDirectoryPath(final String id, final Archive archive, final SamplingWindow samplingWindow) {
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
  protected Optional<String> filePrefix(final String id, final Archive archive, final SamplingWindow samplingWindow) {
    final Collection<String> consolidatorIdentifiers = extractConsolidatorIdentifiers(archive.getConsolidators());
    return Optional.of(Joiner.on("-").join(consolidatorIdentifiers)+"-"+samplingWindow.getDuration()+"@"+samplingWindow.getResolution()+"-");
  }

  /**
   * @param id
   * @param archive
   * @return an optional suffix used when creating file names
   */
  protected Optional<String> fileSuffix(final String id, final Archive archive, final SamplingWindow samplingWindow) {
    return Optional.absent();
  }

  /**
   * @param id
   * @param archive
   * @param samplingWindow 
   * @return an initialized {@link Journal} dedicated to this {@code id} / {@code archive} / {@code samplingWindow} tuple
   * @throws IOException 
   */
  protected Journal createJournal(final String id, final Archive archive, final SamplingWindow samplingWindow) throws IOException {
    final Journal journal = new Journal();
    final String mainDirectory = mainDirectoryPath(id, archive, samplingWindow);
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
    final Optional<String> filePrefix = filePrefix(id, archive, samplingWindow);
    if (filePrefix.isPresent()) {
      journal.setFilePrefix(filePrefix.get());
    }
    final Optional<String> fileSuffix = fileSuffix(id, archive, samplingWindow);
    if (fileSuffix.isPresent()) {
      journal.setFileSuffix(fileSuffix.get());
    }
    //Do not archive deleted entries
    journal.setArchiveFiles(false);
    journal.setChecksum(true);
    journal.setRecoveryErrorHandler(RecoveryErrorHandler.ABORT);
    journal.setPhysicalSync(true);
    journal.setMaxFileLength(JournalIOStorageFactory.MAX_FILE_LENGTH);
    journal.setWriter(this.writerExecutor);
    //journal.setDisposeInterval()
    journal.setDisposer(this.disposerScheduledExecutorService);
    journal.open();
    return journal;
  }

  @Override
  public final Storage createOrOpen(final String id, final Archive archive, final SamplingWindow samplingWindow) throws IOException {
    Preconditions.checkNotNull(id, "null id");
    Preconditions.checkNotNull(archive, "null archive");
    Preconditions.checkNotNull(samplingWindow, "null samplingWindow");

    return new JournalIOStorage(createJournal(id, archive, samplingWindow));
  }

}