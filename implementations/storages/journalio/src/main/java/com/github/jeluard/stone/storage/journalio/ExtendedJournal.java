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

import com.google.common.base.Preconditions;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import journal.io.api.Journal;
import journal.io.api.RecoveryErrorHandler;
import journal.io.api.ReplicationTarget;

/**
 * Extends {@link Journal} by providing builder style API.
 */
public class ExtendedJournal extends Journal {

  public static final class Builder {

    private final File directory;
    private File directoryArchive;
    private Boolean checksum;
    private Long disposeInterval;
    private ScheduledExecutorService disposer;
    private String filePrefix;
    private String fileSuffix;
    private Integer maxFileLength;
    private Integer maxWriteBatchSize;
    private Boolean physicalSync;
    private RecoveryErrorHandler recoveryErrorHandler;
    private ReplicationTarget replicationTarget;
    private Executor writer;

    private Builder(final File directory) {
      //Checks directory exists and is a directory
      //Also ensures current user has write access
      if (!directory.exists()) {
        throw new IllegalArgumentException("<"+directory+"> does not exist");
      }
      if (!directory.isDirectory()) {
        throw new IllegalArgumentException("<"+directory+"> is not a directory");
      }
      if (!directory.canWrite()) {
        throw new IllegalArgumentException("Cannot write to main directory <"+directory+">");
      }

      this.directory = Preconditions.checkNotNull(directory, "null directory");
    }

    public ExtendedJournal.Builder setArchived(final File to) {
      this.directoryArchive = Preconditions.checkNotNull(to, "null to");
      return this;
    }

    public ExtendedJournal.Builder setChecksum(final Boolean checksum) {
      this.checksum = checksum;
      return this;
    }

    public ExtendedJournal.Builder setDisposeInterval(final Long disposeInterval) {
      this.disposeInterval = disposeInterval;
      return this;
    }

    public ExtendedJournal.Builder setDisposer(final ScheduledExecutorService disposer) {
      this.disposer = Preconditions.checkNotNull(disposer, "null disposer");
      return this;
    }

    public ExtendedJournal.Builder setFilePrefix(final String filePrefix) {
      this.filePrefix = Preconditions.checkNotNull(filePrefix, "null filePrefix");
      return this;
    }

    public ExtendedJournal.Builder setFileSuffix(final String fileSuffix) {
      this.fileSuffix = Preconditions.checkNotNull(fileSuffix, "null fileSuffix");
      return this;
    }

    public ExtendedJournal.Builder setMaxFileLength(final Integer maxFileLength) {
      this.maxFileLength = maxFileLength;
      return this;
    }

    public ExtendedJournal.Builder setMaxWriteBatchSize(final Integer maxWriteBatchSize) {
      this.maxWriteBatchSize = maxWriteBatchSize;
      return this;
    }

    public ExtendedJournal.Builder setPhysicalSync(final Boolean physicalSync) {
      this.physicalSync = physicalSync;
      return this;
    }

    public ExtendedJournal.Builder setRecoveryErrorHandler(final RecoveryErrorHandler recoveryErrorHandler) {
      this.recoveryErrorHandler = Preconditions.checkNotNull(recoveryErrorHandler, "null recoveryErrorHandler");
      return this;
    }

    public ExtendedJournal.Builder setReplicationTarget(final ReplicationTarget replicationTarget) {
      this.replicationTarget = Preconditions.checkNotNull(replicationTarget, "null replicationTarget");
      return this;
    }

    public ExtendedJournal.Builder setWriter(final Executor writer) {
      this.writer = Preconditions.checkNotNull(writer, "null writer");
      return this;
    }

    /**
     * @return a configured and opened {@link Journal}
     * @throws IOException 
     */
    public Journal open() throws IOException {
      final Journal journal = new Journal();
      journal.setDirectory(this.directory);
      if (this.directoryArchive != null) {
        journal.setArchiveFiles(true);
        journal.setDirectoryArchive(this.directoryArchive);
      }
      if (this.checksum != null) {
        journal.setChecksum(this.checksum);
      }
      if (this.disposeInterval != null) {
        journal.setDisposeInterval(this.disposeInterval);
      }
      journal.setDisposer(this.disposer);
      journal.setFilePrefix(this.filePrefix);
      journal.setFileSuffix(this.fileSuffix);
      if (this.maxFileLength  != null) {
        journal.setMaxFileLength(this.maxFileLength);
      }
      if (this.maxWriteBatchSize != null) {
        journal.setMaxWriteBatchSize(this.maxWriteBatchSize);
      }
      if (this.physicalSync != null) {
        journal.setPhysicalSync(this.physicalSync);
      }
      journal.setRecoveryErrorHandler(this.recoveryErrorHandler);
      journal.setReplicationTarget(this.replicationTarget);
      journal.setWriter(this.writer);
      journal.open();
      return journal;
    }

  }

  /**
   * @param directory
   * @return a {@link Builder} using {@code directory} as base directory
   */
  public static ExtendedJournal.Builder of(final File directory) {
    return new Builder(directory);
  }

}