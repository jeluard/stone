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
package com.github.jeluard.stone.dispatcher.blocking_queue;

import com.github.jeluard.guayaba.util.concurrent.ThreadFactoryBuilders;
import com.github.jeluard.stone.api.Consolidator;
import com.github.jeluard.stone.api.Listener;
import com.github.jeluard.stone.helper.Loggers;
import com.github.jeluard.stone.spi.Dispatcher;
import com.google.common.base.Preconditions;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * {@link Dispatcher} implementation executing {@link Dispatcher#accumulateAndPersist(com.github.jeluard.stone.api.Window, com.github.jeluard.stone.api.Consolidator[], long, long, long, int)}
 * through a {@link ExecutorService} waiting for new execution to be available via a {@link BlockingQueue}.
 */
public class BlockingQueueDispatcher extends Dispatcher {

  /**
   * Holder for {@link Listener#onPublication(long, long, int)} invocation details.
   */
  public static final class Entry {

    public final long previousTimestamp;
    public final long currentTimestamp;
    public final int value;
    public final Listener listener;

    public Entry(final long previousTimestamp, final long currentTimestamp, final int value, final Listener listener) {
      this.previousTimestamp = previousTimestamp;
      this.currentTimestamp = currentTimestamp;
      this.value = value;
      this.listener = listener;
    }

  }

  private final class Consumer implements Runnable {
    @Override
    public void run() {
      try {
        while (true) {
          final Entry entry = BlockingQueueDispatcher.this.queue.take();
          try {
            entry.listener.onPublication(entry.previousTimestamp, entry.currentTimestamp, entry.value);
          } catch (Exception e) {
            notifyExceptionHandler(e);
          }
        }
      } catch (InterruptedException e) {
        if (BlockingQueueDispatcher.LOGGER.isLoggable(Level.INFO)) {
          BlockingQueueDispatcher.LOGGER.log(Level.INFO, "Consumer <{0}> interrupted; letting it die", Thread.currentThread().getName());
        }
      }
    }
  }

  private static final Logger LOGGER = Loggers.create("dispatcher.blocking-queue");

  private final BlockingQueue<Entry> queue;
  private final ExecutorService executorService;
  private static final String CONSUMERS_THREAD_NAME_FORMAT = "BlockingQueueDispatcher-Consumers #%d";

  public BlockingQueueDispatcher(final BlockingQueue<Entry> queue, final ExecutorService executorService, final int consumers) {
    this(queue, executorService, consumers, Dispatcher.DEFAULT_EXCEPTION_HANDLER);
  }

  public BlockingQueueDispatcher(final BlockingQueue<Entry> queue, final ExecutorService executorService, final int consumers, final ExceptionHandler exceptionHandler) {
    super(exceptionHandler);

    this.queue = Preconditions.checkNotNull(queue, "null queue");
    this.executorService = Preconditions.checkNotNull(executorService, "null executorService");

    for (int i = 0; i < consumers; i++) {
      this.executorService.submit(new Consumer());
    }
  }

  /**
   * @return an {@link ExecutorService} with a fixed thread pool of {@link Runtime#availableProcessors()} threads
   */
  public static ExecutorService defaultExecutorService() {
    final ThreadFactory threadFactory = ThreadFactoryBuilders.safeBuilder(BlockingQueueDispatcher.CONSUMERS_THREAD_NAME_FORMAT, BlockingQueueDispatcher.LOGGER).build();
    return Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors(), threadFactory);
  }

  @Override
  public boolean dispatch(final long previousTimestamp, final long currentTimestamp, final int value, final Listener[] listeners) {
    for (final Listener listener : listeners) {
      final boolean added = this.queue.offer(new Entry(previousTimestamp, currentTimestamp, value, listener));
      if (!added) {
        return false;
      }
    }
    return true;
  }

}