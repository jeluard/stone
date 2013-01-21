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
package com.github.jeluard.stone.api;

import com.github.jeluard.guayaba.base.Pair;
import com.github.jeluard.stone.impl.Engine;
import com.github.jeluard.stone.spi.Dispatcher;
import com.github.jeluard.stone.spi.Storage;
import com.github.jeluard.stone.spi.StorageFactory;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

import org.joda.time.DateTime;
import org.joda.time.Interval;

public final class TimeSeries implements Closeable {

  private static final Set<String> IDS = new CopyOnWriteArraySet<String>();

  private final String id;
  private final Engine engine;
  private long beginning;
  private long latest;

  public TimeSeries(final String id, final Collection<Archive> archives, final StorageFactory storageFactory) throws IOException {
    this.id = Preconditions.checkNotNull(id, "null id");
    if (!TimeSeries.IDS.add(id)) {
      throw new IllegalArgumentException("ID <"+id+"> is already used");
    }
    this.engine = new Engine(id, archives, storageFactory);
    final Interval span = engine.span();
    this.beginning = span.getStartMillis();
    this.latest = span.getEndMillis();
  }

  public String getId() {
    return this.id;
  }

  private void checkNotBeforeLatestTimestamp(final long previousTimestamp, final long currentTimestamp) {
    if (!(currentTimestamp > previousTimestamp)) {
      throw new IllegalArgumentException("Provided timestamp from <"+currentTimestamp+"> must be more recent than <"+previousTimestamp+">");
    }
  }

  private long recordLatest(final long timestamp) {
    //Set to the new timestamp if value is null (i.e. no value as yet been recorded)
    final long previousTimestamp = this.latest;
    this.latest = timestamp;
    checkNotBeforeLatestTimestamp(previousTimestamp, timestamp);
    return previousTimestamp;
  }

  private long inferBeginning(final long timestamp) {
    //If beginning is still null (i.e. all storages where empty) sets it's value to timestamp
    //This will be done only once (hence track the first timestamp received)
    if (this.beginning == 0L) {
      this.beginning = timestamp;
    }
    return this.beginning;
  }

  public void publish(final long timestamp, final int value) throws IOException {
    Preconditions.checkNotNull(timestamp, "null timestamp");

    final long previousTimestamp = recordLatest(timestamp);
    final long beginningTimestamp = inferBeginning(timestamp);

    this.engine.publish(beginningTimestamp, previousTimestamp, timestamp, value);
  }

  /**
   * Calls {@link Closeable#close()} on all {@link Storage} implementing {@link Closeable}.
   *
   * @throws IOException 
   */
  @Override
  public void close() throws IOException {
    TimeSeries.IDS.remove(this.id);

    this.engine.close();
  }

}