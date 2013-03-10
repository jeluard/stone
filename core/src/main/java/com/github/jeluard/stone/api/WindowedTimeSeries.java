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

import com.github.jeluard.guayaba.base.Preconditions2;
import com.github.jeluard.stone.helper.Consolidators;
import com.github.jeluard.stone.spi.Dispatcher;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * A {@link TimeSeries} than trigger a consolidation process when {@link Window} boundaries are crossed.
 */
public class WindowedTimeSeries extends TimeSeries {

  /**
   * {@link Listener} implementation embedding windowing logic.
   */
  protected static final class WindowListener implements Listener {

    private final int size;
    private static final long DEFAULT_BEGINNING_TIMESTAMP = 0L;
    private volatile long beginningTimestampOrDefault = WindowListener.DEFAULT_BEGINNING_TIMESTAMP;
    private final Consolidator[] consolidators;
    private final int[] consolidates;
    private final ConsolidationListener consolidationListener;

    public WindowListener(final int size, final int granularity, final Optional<Long> latestTimestamp, final ConsolidationListener consolidationListener, final List<? extends Class<? extends Consolidator>> consolidatorTypes) {
      this.size =  Preconditions2.checkSize(size);
      if (Preconditions.checkNotNull(latestTimestamp, "null latestTimestamp").isPresent()) {
        this.beginningTimestampOrDefault = latestTimestamp.get() - size;
      }
      final int maxSamples = size / granularity;
      this.consolidators = Consolidators.createConsolidators(consolidatorTypes, maxSamples);
      this.consolidationListener = Preconditions.checkNotNull(consolidationListener, "null consolidationListener");
      this.consolidates = new int[this.consolidators.length];
    }

    /**
     * Propagates {@code timestamp}/{@code value} to all {@code consolidators} {@link Consolidator#accumulate(long, int)}.
     *
     * @param timestamp
     * @param value 
     */
    private void accumulate(final long timestamp, final int value) {
      for (final Consolidator consolidator : this.consolidators) {
        consolidator.accumulate(timestamp, value);
      }
    }

    /**
     * Generate all consolidates.
     *
     * @return consolidates
     */
    private int[] generateConsolidates() {
      for (int i = 0; i < this.consolidates.length; i++) {
        this.consolidates[i] = this.consolidators[i].consolidateAndReset();
      }
      return this.consolidates;
    }

    private void generateConsolidatesThenNotify(final long timestamp) {
      this.consolidationListener.onConsolidation(timestamp, generateConsolidates());
    }

    /**
     * @param timestamp
     * @return id of the {@link Window} containing {@code timestamp}; starts at 1
     */
    private long windowId(final long timestamp) {
      return 1 + (timestamp - this.beginningTimestampOrDefault) / this.size;
    }

    private long windowEnd(final long windowId) {
      return this.beginningTimestampOrDefault + windowId * this.size - 1;
    }

    private boolean isLatestFromWindow(final long timestamp) {
      final long elapsed = timestamp - this.beginningTimestampOrDefault;
      return elapsed != 0 && ((elapsed+1) % this.size == 0);
    }

    private void recordBeginningTimestampIfNeeded(final long previousTimestamp, final long currentTimestamp) {
      if (previousTimestamp == TimeSeries.DEFAULT_LATEST_TIMESTAMP) {
        this.beginningTimestampOrDefault = currentTimestamp;
      }
    }

    /**
     * Call {@link #generateConsolidatesThenNotify(long)} if {@link Window} threshold crossed and {@link #accumulate(long, int)} {@code value}.
     *
     * 1st case: current timestamp *is* latest from this window and belongs to previous' window: persist after consolidation of current
     * 2nd case: current timestamp is in a new window 
     *  2-1 previous *is not* the latest from previous window : persist before consolidation of current (existing stuff)
     *  2-2 previous is latest from previous window: do not persist as we already did it during previous reception
     *
     * 2-2 allows to handle restart from existing storage (where previous will be latest from previous window)
     *
     * @param previousTimestamp 
     * @param currentTimestamp
     * @param value 
     */
    @Override
    public void onPublication(final long previousTimestamp, final long currentTimestamp, final int value) {
      recordBeginningTimestampIfNeeded(previousTimestamp, currentTimestamp);
      final long currentWindowId = windowId(currentTimestamp);
      final long previousWindowId = windowId(previousTimestamp);
      final boolean newWindow = previousWindowId >= 0 && currentWindowId != previousWindowId;
      //New window, previous timestamp didn't trigger a consolidation (wasn't last slot): trigger it now.
      if (newWindow) {
        if (!isLatestFromWindow(previousTimestamp)) {
          final long previousWindowEnd = windowEnd(previousWindowId);
          generateConsolidatesThenNotify(previousWindowEnd);
        }
      }

      accumulate(currentTimestamp, value);

      //Last slot: trigger a consolidation.
      if (isLatestFromWindow(currentTimestamp)) {
        generateConsolidatesThenNotify(currentTimestamp);
      }
    }
    
  }

  /**
   * @param id
   * @param granularity
   * @param windows
   * @param dispatcher 
   */
  public WindowedTimeSeries(final String id, final int granularity, final List<Window> windows, final Dispatcher dispatcher) throws IOException {
    super(id, granularity, extractLatestIfAny(windows), createWrappedConsolidationListeners(id, granularity, windows), dispatcher);
  }

  private static Optional<Long> extractLatestIfAny(final List<Window> windows) throws IOException {
    Long timestamp = null;
    for (final Window window : windows) {
      for (final ConsolidationListener consolidationListener  : window.getConsolidationListeners()) {
        if (consolidationListener instanceof ConsolidationListener.Persistent) {
          final Optional<Long> latestTimestampOptional = ((ConsolidationListener.Persistent) consolidationListener).getLatestTimestamp();
          if (!latestTimestampOptional.isPresent()) {
            continue;
          }

          final long latestTimestamp = latestTimestampOptional.get();
          if (timestamp == null || latestTimestamp > timestamp) {
            timestamp = latestTimestamp;
          }
        }
      }
    }
    return Optional.<Long>fromNullable(timestamp);
  }

  protected static List<WindowListener> createWrappedConsolidationListeners(final String id, final int granularity, final List<Window> windows) throws IOException {
    final List<WindowListener> windowListeners = new ArrayList<WindowListener>(windows.size());
    for (final Window window : windows) {
      final Optional<Long> latestTimestamp = extractLatestIfAny(Arrays.asList(window));
      for (final ConsolidationListener consolidationListener : window.getConsolidationListeners()) {
        windowListeners.add(new WindowListener(window.getSize(), granularity, latestTimestamp, consolidationListener, window.getConsolidatorTypes()));
      }
    }
    return windowListeners;
  }

  public final Iterable<WindowListener> getWindowListeners() {
    return Iterables.filter(getListeners(), WindowListener.class);
  }

  @Override
  protected void cleanup() throws IOException {
    final long latestTimestamp = getLatestTimestamp();
    for (final WindowListener windowListener : getWindowListeners()) {
      //If a window holds data not flushed (to listeners) trigger notification
      //This is the case when latestTimestamp is not the latest possible value of a window
      if (!windowListener.isLatestFromWindow(latestTimestamp)) {
        final long windowEnd = windowListener.windowEnd(windowListener.windowId(latestTimestamp));
        windowListener.generateConsolidatesThenNotify(windowEnd);
      }
    }
  }

}