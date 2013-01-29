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
package com.github.jeluard.stone.dispatcher.disruptor;

import com.github.jeluard.guayaba.base.Preconditions2;
import com.github.jeluard.guayaba.lang.Cancelable;
import com.github.jeluard.guayaba.lang.UncaughtExceptionHandlers;
import com.github.jeluard.stone.api.ConsolidationListener;
import com.github.jeluard.stone.api.Consolidator;
import com.github.jeluard.stone.api.Window;
import com.github.jeluard.stone.helper.Loggers;
import com.github.jeluard.stone.spi.Dispatcher;
import com.github.jeluard.stone.spi.Storage;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.lmax.disruptor.AbstractMultithreadedClaimStrategy;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.MultiThreadedLowContentionClaimStrategy;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.SleepingWaitStrategy;
import com.lmax.disruptor.WaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;

import java.io.IOException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.logging.Logger;

public class DisruptorDispatcher extends Dispatcher implements Cancelable {

  /**
   * {@link RingBuffer} event encapsulating a {@link DataPoint}.
   */
  private static final class Event {

    private volatile Window window;
    private volatile Storage storage;
    private volatile Consolidator[] consolidators;
    private volatile ConsolidationListener[] consolidationListeners;
    private volatile long beginningTimestamp;
    private volatile long previousTimestamp;
    private volatile long currentTimestamp;
    private volatile int value;

    /**
     * Unique {@link EventFactory} instance for all published arguments.
     */
    private static final EventFactory<Event> EVENT_FACTORY = new EventFactory<Event>() {
      @Override
      public Event newInstance() {
        return new Event();
      }
    };

  }

  private static final Logger LOGGER = Loggers.create("dispatcher.disruptor");

  private final Disruptor<Event> disruptor;
  private static final String THREAD_NAME_FORMAT = "DisruptorDispatcher #%d";

  public DisruptorDispatcher(final Executor executor, final int bufferSize) {
    this(executor, new MultiThreadedLowContentionClaimStrategy(Preconditions2.checkSize(bufferSize)), new SleepingWaitStrategy());
  }

  public DisruptorDispatcher(final Executor executor, final AbstractMultithreadedClaimStrategy claimStrategy, final WaitStrategy waitStrategy) {
    Preconditions.checkNotNull(executor, "null executor");
    Preconditions.checkNotNull(claimStrategy, "null claimStrategy");
    Preconditions.checkNotNull(waitStrategy, "null waitStrategy");

    this.disruptor = new Disruptor<Event>(Event.EVENT_FACTORY, executor, claimStrategy, waitStrategy);
    this.disruptor.handleEventsWith(new EventHandler<Event>() {
      @Override
      public void onEvent(final Event event, final long sequence, final boolean endOfBatch) throws Exception {
        accumulateAndPersist(event.window, event.storage, event.consolidators, event.consolidationListeners, event.beginningTimestamp, event.previousTimestamp, event.currentTimestamp, event.value);
      }
    });
    this.disruptor.start();
  }

  /**
   * @return an {@link ExecutorService} with a fixed thread pool of {@link Runtime#availableProcessors()} threads
   */
  public static ExecutorService defaultExecutorService() {
    final ThreadFactory threadFactory = new ThreadFactoryBuilder().setDaemon(true).setNameFormat(DisruptorDispatcher.THREAD_NAME_FORMAT).setUncaughtExceptionHandler(UncaughtExceptionHandlers.defaultHandler(DisruptorDispatcher.LOGGER)).build();
    return Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors(), threadFactory);
  }

  @Override
  protected boolean dispatch(final Window window, final Storage storage, final Consolidator[] consolidators, final ConsolidationListener[] consolidationListeners, final long beginningTimestamp, final long previousTimestamp, final long currentTimestamp, final int value) throws IOException {
    final RingBuffer<Event> ringBuffer = this.disruptor.getRingBuffer();
    final long sequence = ringBuffer.next();
    final Event event = ringBuffer.get(sequence);
    event.window = window;
    event.storage = storage;
    event.consolidators = consolidators;
    event.consolidationListeners = consolidationListeners;
    event.beginningTimestamp = beginningTimestamp;
    event.previousTimestamp = previousTimestamp;
    event.currentTimestamp = currentTimestamp;
    event.value = value;

    ringBuffer.publish(sequence);

    return true;
  }

  @Override
  public void cancel() {
    //TODO force append stop
    this.disruptor.shutdown();
  }

}