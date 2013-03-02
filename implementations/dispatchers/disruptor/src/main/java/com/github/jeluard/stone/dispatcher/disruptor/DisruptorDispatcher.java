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
import com.github.jeluard.guayaba.util.concurrent.ThreadFactoryBuilders;
import com.github.jeluard.stone.api.Listener;
import com.github.jeluard.stone.helper.Loggers;
import com.github.jeluard.stone.spi.Dispatcher;
import com.google.common.base.Preconditions;
import com.lmax.disruptor.AbstractMultithreadedClaimStrategy;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.MultiThreadedLowContentionClaimStrategy;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.SleepingWaitStrategy;
import com.lmax.disruptor.WaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;

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

    private volatile long previousTimestamp;
    private volatile long currentTimestamp;
    private volatile int value;
    private volatile Listener listener;

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

  static final Logger LOGGER = Loggers.create("dispatcher.disruptor");

  private final Disruptor<Event> disruptor;
  private static final String THREAD_NAME_FORMAT = "DisruptorDispatcher #%d";

  public DisruptorDispatcher(final Executor executor, final int bufferSize) {
    this(executor, new MultiThreadedLowContentionClaimStrategy(Preconditions2.checkSize(bufferSize)), new SleepingWaitStrategy(), Dispatcher.DEFAULT_EXCEPTION_HANDLER);
  }

  public DisruptorDispatcher(final Executor executor, final AbstractMultithreadedClaimStrategy claimStrategy, final WaitStrategy waitStrategy, final ExceptionHandler exceptionHandler) {
    super(exceptionHandler);

    Preconditions.checkNotNull(executor, "null executor");
    Preconditions.checkNotNull(claimStrategy, "null claimStrategy");
    Preconditions.checkNotNull(waitStrategy, "null waitStrategy");

    this.disruptor = new Disruptor<Event>(Event.EVENT_FACTORY, executor, claimStrategy, waitStrategy);
    this.disruptor.handleExceptionsWith(new CustomExceptionHandler(){
      @Override
      public void handleEventException(final Throwable exception, final long sequence, final Object event) {
        if (!(exception instanceof Exception)) {
          super.handleEventException(exception, sequence, event);
          return;
        }

        notifyExceptionHandler((Exception) exception);
      }
    });
    this.disruptor.handleEventsWith(new EventHandler<Event>() {
      @Override
      public void onEvent(final Event event, final long sequence, final boolean endOfBatch) throws Exception {
        event.listener.onPublication(event.previousTimestamp, event.currentTimestamp, event.value);
      }
    });
    this.disruptor.start();
  }

  /**
   * @return an {@link ExecutorService} with a fixed thread pool of {@link Runtime#availableProcessors()} threads
   */
  public static ExecutorService defaultExecutorService() {
    final ThreadFactory threadFactory = ThreadFactoryBuilders.safeBuilder(DisruptorDispatcher.THREAD_NAME_FORMAT, DisruptorDispatcher.LOGGER).build();
    return Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors(), threadFactory);
  }

  @Override
  public boolean dispatch(final long previousTimestamp, final long currentTimestamp, final int value, final Listener[] listeners) {
    final RingBuffer<Event> ringBuffer = this.disruptor.getRingBuffer();

    for (final Listener listener : listeners) {
      final long sequence = ringBuffer.next();
      final Event event = ringBuffer.get(sequence);
      event.previousTimestamp = previousTimestamp;
      event.currentTimestamp = currentTimestamp;
      event.value = value;
      event.listener = listener;

      ringBuffer.publish(sequence);
    }

    return true;
  }

  @Override
  public void cancel() {
    //TODO force append stop
    this.disruptor.shutdown();
  }

}