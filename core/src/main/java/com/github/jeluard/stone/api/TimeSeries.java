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

import com.github.jeluard.guayaba.annotation.Idempotent;
import com.github.jeluard.guayaba.lang.Identifiable;
import com.github.jeluard.stone.spi.Dispatcher;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Main abstraction allowing to publish {@code timestamp}/{@code value} pair. Provided {@code timestamp} must monotonically increased and compatible with {@code granularity}.
 * <br>
 * If {@code granularity} is {@code 1} then {@code timestamp} can be any value (as long as it follows the monotonicity rule).
 * <br>
 * If {@code granularity} is greater than {@code 1} each successive {@code timestamp}s must have {@code granularity - 1} values in between. Note this does not imply only {@code granularity} multiples are accepted as there can be more than {@code granularity - 1} in between two timestamps.
 * <br>
 * An invalid {@code timestamp} will be signaled via return value of {@link #publish(long, int)}.
 * <br>
 * <br>
 * Each accepted value then is passed to all associated {@link Listener}.
 * <br>
 * <br>
 * Once {@link #close()} has been called {@link #publish(long, int)} behaviour is undefined.
 * <br>
 * <br>
 * <b>WARNING</b> {@link TimeSeries} is not threadsafe and not supposed to be manipulated concurrently from different threads.
 */
@NotThreadSafe
public class TimeSeries implements Identifiable<String>, Closeable {

  private static final Set<String> IDS = new CopyOnWriteArraySet<String>();

  private final String id;
  private final int granularity;
  private final Listener[] listeners;
  private final Dispatcher dispatcher;
  static final long DEFAULT_LATEST_TIMESTAMP = 0L;
  private long latestTimestamp;

  public TimeSeries(final String id, final int granularity, final List<? extends Listener> listeners, final Dispatcher dispatcher) {
    this(id, granularity, Optional.<Long>absent(), listeners, dispatcher);
  }

  public TimeSeries(final String id, final int granularity, final Optional<Long> latestTimestamp, final List<? extends Listener> listeners, final Dispatcher dispatcher) {
    this.id = Preconditions.checkNotNull(id, "null id");
    this.granularity = granularity;
    this.listeners = Preconditions.checkNotNull(listeners, "null listeners").toArray(new Listener[listeners.size()]);
    this.dispatcher = Preconditions.checkNotNull(dispatcher, "null dispatcher");
    this.latestTimestamp = Preconditions.checkNotNull(latestTimestamp, "null latestTimestamp").or(TimeSeries.DEFAULT_LATEST_TIMESTAMP);

    //Last so that rejected timeseries due to invalid argument do not leak their id
    if (!TimeSeries.IDS.add(id)) {
      throw new IllegalArgumentException("Id <"+id+"> is already used");
    }
  }

  /**
   * @return a unique id identifying this {@link TimeSeries} in associated {@link Database}
   */
  @Override
  public final String getId() {
    return this.id;
  }

  protected final Iterable<Listener> getListeners() {
    return new ArrayList<Listener>(Arrays.asList(this.listeners));
  }

  protected final long getLatestTimestamp() {
    return this.latestTimestamp;
  }

  /**
   * @param currentTimestamp
   * @return true if timestamp is after previous one (considering granularity)
   */
  private boolean isAfterPreviousTimestamp(final long currentTimestamp, final long granularity) {
    return (currentTimestamp - this.latestTimestamp) >= granularity;
  }

  /**
   * @param timestamp
   * @return latest {@code timestamp} published; {@linkplain TimeSeries#DEFAULT_LATEST_TIMESTAMP} if does not exist
   */
  private long recordTimestamp(final long timestamp) {
    final long previousTimestamp = this.latestTimestamp;
    this.latestTimestamp = timestamp;
    return previousTimestamp;
  }

  /**
   * Publish a {@code timestamp}/{@code value} pair triggering accumulation/persistency process.
   * <br>
   * {@code timestamp}s must be monotonic (i.e. each timestamp must be strictly greate than the previous one).
   * <br>
   * {@code timestamp}s are assumed to be in milliseconds. No timezone information is considered so {@code timestamp} values must be normalized before being published.
   *
   * @param timestamp
   * @param value
   * @return true if {@code value} has been considered for accumulation (i.e. it is not a value in the past)
   */
  public final boolean publish(final long timestamp, final int value) {
    final boolean isAfter = isAfterPreviousTimestamp(timestamp, this.granularity);
    if (!isAfter) {
      //timestamp is older that what can be accepted; return early
      return false;
    }

    final long previousTimestamp = recordTimestamp(timestamp);

    //TODO propagate result. Using enum?
    this.dispatcher.dispatch(previousTimestamp, timestamp, value, this.listeners);

    return true;
  }

  @Idempotent
  @Override
  public final void close() throws IOException {
    try {
      cleanup();
    } finally {
      TimeSeries.IDS.remove(this.id);
    }
  }

  /**
   * Extension point. Called during {@link #close()}.
   */
  @Idempotent
  protected void cleanup() throws IOException {
  }

}