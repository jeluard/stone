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
package com.github.jeluard.stone.spi;

import com.github.jeluard.guayaba.base.Triple;
import com.github.jeluard.stone.api.ConsolidationListener;
import com.github.jeluard.stone.api.Consolidator;
import com.github.jeluard.stone.api.Window;
import com.github.jeluard.stone.helper.Loggers;
import com.google.common.base.Preconditions;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.annotation.concurrent.ThreadSafe;

/**
 * Encapsulate the logic that triggers call to {@link Consolidator#accumulate(long, int)}, {@link Consolidator#consolidateAndReset()} and {@link Storage#append(long, int[])}.
 * <br>
 * A single {@link Engine} is available per {@link com.github.jeluard.stone.api.Database} thus shared among all associated {@link com.github.jeluard.stone.api.TimeSeries}.
 */
@ThreadSafe
public abstract class Dispatcher {

  /**
   * Intercept rejection while dispatching.
   *
   * @see #dispatch(com.github.jeluard.stone.api.Window, com.github.jeluard.stone.spi.Storage, com.github.jeluard.stone.api.Consolidator[], com.github.jeluard.stone.api.ConsolidationListener[], long, long, long, int) 
   */
  public interface RejectionHandler {

    /**
     * Invoked when a dispatch returns false during publication process.
     *
     * @param window
     * @param storage
     * @param timstamp
     * @param value 
     */
    void onRejection(Window window, Storage storage, long timstamp, int value);

  }

  /**
   * Intercept {@link Exception} thrown when a {@code value} is published.
   */
  public interface ExceptionHandler {

    /**
     * Invoked when an {@link Exception} is thrown during accumulation/persistency process.
     *
     * @param exception
     */
    void onException(Exception exception);

  }

  private static final Logger LOGGER = Loggers.create("dispatcher");

  private final RejectionHandler rejectionHandler;
  protected static final RejectionHandler DEFAULT_REJECTION_HANDLER = new RejectionHandler() {
    @Override
    public void onRejection(final Window window, final Storage storage, final long timestamp, final int value) {
      if (Dispatcher.LOGGER.isLoggable(Level.WARNING)) {
        Dispatcher.LOGGER.log(Level.WARNING, "Rejection of <{0}> <{1}> for <{2}> by <{3}>", new Object[]{timestamp, value, window, storage});
      }
    }
  };
  private final ExceptionHandler exceptionHandler;
  protected static final ExceptionHandler DEFAULT_EXCEPTION_HANDLER = new ExceptionHandler() {
    @Override
    public void onException(final Exception exception) {
      if (Dispatcher.LOGGER.isLoggable(Level.WARNING)) {
        Dispatcher.LOGGER.log(Level.WARNING, "Got exception while publishing", exception);
      }
    }
  };

  public Dispatcher(final RejectionHandler rejectionHandler, final ExceptionHandler exceptionHandler) {
    this.rejectionHandler = Preconditions.checkNotNull(rejectionHandler, "null rejectionHandler");
    this.exceptionHandler = Preconditions.checkNotNull(exceptionHandler, "null exceptionHandler");
  }

  /**
   * Safely execute {@link RejectionHandler#onRejection(com.github.jeluard.stone.api.Window, com.github.jeluard.stone.spi.Storage, long, int)} 
   *
   * @param window 
   * @param storage 
   * @param timestamp 
   * @param value 
   */
  private void notifyRejectionHandler(final Window window, final Storage storage, final long timestamp, final int value) {
    try {
      this.rejectionHandler.onRejection(window, storage, timestamp, value);
    } catch (Exception e) {
      if (Dispatcher.LOGGER.isLoggable(Level.WARNING)) {
        Dispatcher.LOGGER.log(Level.WARNING, "Got exception while executing "+RejectionHandler.class.getSimpleName()+" <"+this.exceptionHandler+">", e);
      }
    }
  }

  /**
   * Safely execute {@link ExceptionHandler#onException(com.github.jeluard.stone.api.Window, com.github.jeluard.stone.api.Consolidator[], java.lang.Exception)} 
   *
   * @param exception 
   */
  protected final void notifyExceptionHandler(final Exception exception) {
    try {
      this.exceptionHandler.onException(exception);
    } catch (Exception e) {
      if (Dispatcher.LOGGER.isLoggable(Level.WARNING)) {
        Dispatcher.LOGGER.log(Level.WARNING, "Got exception while executing "+ExceptionHandler.class.getSimpleName()+" <"+this.exceptionHandler+">", e);
      }
    }
  }

  /**
   * @param beginning
   * @param timestamp
   * @param duration
   * @return id of the {@link Window} containing {@code timestamp}
   */
  private long windowId(final long beginning, final long timestamp, final long duration) {
    return (timestamp - beginning) / duration;
  }

  /**
   * Propagates {@code timestamp}/{@code value} to all {@code consolidators} {@link Consolidator#accumulate(long, int)}.
   *
   * @param consolidators
   * @param timestamp
   * @param value 
   */
  private void accumulate(final Consolidator[] consolidators, final long timestamp, final int value) {
    for (final Consolidator consolidator : consolidators) {
      consolidator.accumulate(timestamp, value);
    }
  }

  /**
   * Generate all consolidates then propagates them to {@link Storage#append(long, int[])}.
   * <br>
   * If any, {@link ConsolidationListener#onConsolidation(com.github.jeluard.stone.api.Window, long, int[])} are then called.
   * Failure of one of {@link ConsolidationListener#onConsolidation(com.github.jeluard.stone.api.Window, long, int[])} does not prevent others to be called.
   *
   * @param timestamp
   * @param consolidators
   * @param storage
   * @return consolidates
   * @throws IOException 
   */
  private int[] persist(final long timestamp, final Consolidator[] consolidators, final Storage storage) throws IOException {
    //TODO Do not create arrays each time?
    final int[] consolidates = new int[consolidators.length];
    for (int i = 0; i < consolidators.length; i++) {
      consolidates[i] = consolidators[i].consolidateAndReset();
    }
    storage.append(timestamp, consolidates);
    return consolidates;
  }

  /**
   * @param timestamp
   * @param consolidates
   * @param consolidationListeners
   * @param window passed to {@link ConsolidationListener#onConsolidation(com.github.jeluard.stone.api.Window, long, int[])}
   */
  private void notifyConsolidationListeners(final long timestamp, final int[] consolidates, final ConsolidationListener[] consolidationListeners, final Window window) {
    for (final ConsolidationListener consolidationListener : consolidationListeners) {
      try {
        consolidationListener.onConsolidation(window, timestamp, consolidates);
      } catch (Exception e)  {
        if (Dispatcher.LOGGER.isLoggable(Level.WARNING)) {
          Dispatcher.LOGGER.log(Level.WARNING, "Got exception while executing <"+consolidationListener+">", e);
        }
      }
    }
  }

  private long windowBeginning(final long timestamp, final long windowId, final long duration) {
    return timestamp + windowId * duration;
  }

  private boolean isLatestFromWindow(final long timestamp, final long beginning, final long duration) {
    return (timestamp - beginning) % duration == 0;
  }

  /**
   * Call {@link #persist(long, com.github.jeluard.stone.api.Consolidator[], com.github.jeluard.stone.spi.Storage)} if {@link Window} threshold crossed then {@link #accumulate(com.github.jeluard.stone.api.Consolidator[], long, int)} {@code value}.
   *
   * 1st case: current timestamp *is* latest from this window and belongs to previous' window: persist after consolidation of current
   * 2nd case: current timestamp is in a new window 
   *  2-1 previous *is not* the latest from previous window : persist before consolidation of current (existing stuff)
   *  2-2 previous is latest from previous window: do not persist as we already did it during previous reception
   *
   * 2-2 allows to handle restart from existing storage (where previous will be latest from previous window)
   *
   * @param window
   * @param storage
   * @param consolidators
   * @param consolidationListeners
   * @param beginningTimestamp
   * @param previousTimestamp
   * @param currentTimestamp
   * @param value
   * @throws IOException 
   */
  protected final void persistAndAccumulate(final Window window, final Storage storage, final Consolidator[] consolidators, final ConsolidationListener[] consolidationListeners, final long beginningTimestamp, final long previousTimestamp, final long currentTimestamp, final int value) throws IOException {
    final long duration = window.getResolution().getMillis();
    final long currentWindowId = windowId(beginningTimestamp, currentTimestamp, duration);
    final long previousWindowId = windowId(beginningTimestamp, previousTimestamp, duration);
    if (currentWindowId != previousWindowId) {
      if (!isLatestFromWindow(previousTimestamp, beginningTimestamp, duration)) {
        final long previousWindowBeginning = windowBeginning(beginningTimestamp, previousWindowId, duration);

        final int[] consolidates = persist(previousWindowBeginning, consolidators, storage);
        notifyConsolidationListeners(previousWindowBeginning, consolidates, consolidationListeners, window);
      }
    }

    accumulate(consolidators, currentTimestamp, value);

    if (currentWindowId == previousWindowId && isLatestFromWindow(currentTimestamp, beginningTimestamp, duration)) {
      final long currentWindowBeginning = windowBeginning(beginningTimestamp, currentWindowId, duration);

      final int[] consolidates = persist(currentWindowBeginning, consolidators, storage);
      notifyConsolidationListeners(currentWindowBeginning, consolidates, consolidationListeners, window);
    }
  }

  /**
   * Perform {@link Consolidator#accumulate(long, int)} and {@link Consolidator#consolidateAndReset()} for all {@link Window} depending on {@code timestamp}.
   * <br>
   * Result of {@link Consolidator#consolidateAndReset()} is then persisted (via {@link Storage#append(long, int[])}) through the right {@link Storage}.
   * <br>
   * <br>
   * When provided {@link ConsolidationListener}s are called <strong>after</strong> a successful {@link Storage#append(long, int[])}.
   *
   * @param triples
   * @param consolidationListeners
   * @param beginningTimestamp
   * @param previousTimestamp
   * @param currentTimestamp
   * @param value
   */
  public final void publish(final Triple<Window, Storage, Consolidator[]>[] triples, final ConsolidationListener[] consolidationListeners, final long beginningTimestamp, final long previousTimestamp, final long currentTimestamp, final int value) {
    //Note: triples could be factored by Window#getResolution() to limit window threshold crossing checks.
    //Given the low probability several Window have same resolution but different duration this optimisation is not considered to keep implementation simple.
    for (final Triple<Window, Storage, Consolidator[]> triple : triples) {
      if (!dispatch(triple.first, triple.second, triple.third, consolidationListeners, beginningTimestamp, previousTimestamp, currentTimestamp, value)) {
        notifyRejectionHandler(triple.first, triple.second, currentTimestamp, value);
      }
    }
  }

  /**
   * Dispatch a {@link #publish(com.github.jeluard.guayaba.base.Triple<com.github.jeluard.stone.api.Window,com.github.jeluard.stone.spi.Storage,com.github.jeluard.stone.api.Consolidator[]>[], com.github.jeluard.stone.api.ConsolidationListener[], long, long, long, int)} call.
   * To be effective the call must be delegated to {@link #accumulateAndPersist(com.github.jeluard.stone.api.Window, com.github.jeluard.stone.spi.Storage, com.github.jeluard.stone.api.Consolidator[], com.github.jeluard.stone.api.ConsolidationListener[], long, long, long, int)}.
   * <br>
   * Must be thread-safe.
   *
   * @param window
   * @param storage
   * @param consolidators
   * @param consolidationListeners
   * @param beginningTimestamp
   * @param previousTimestamp
   * @param currentTimestamp
   * @param value
   * @return true if dispatch has been accepted; false if rejected
   */
  protected abstract boolean dispatch(Window window, Storage storage, Consolidator[] consolidators, ConsolidationListener[] consolidationListeners, long beginningTimestamp, long previousTimestamp, long currentTimestamp, int value);

}