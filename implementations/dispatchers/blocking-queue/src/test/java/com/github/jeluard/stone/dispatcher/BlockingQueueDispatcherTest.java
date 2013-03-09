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
package com.github.jeluard.stone.dispatcher;

import com.github.jeluard.stone.dispatcher.blocking_queue.BlockingQueueDispatcher;
import com.github.jeluard.stone.spi.BaseDispatcherTest;
import com.github.jeluard.stone.spi.Dispatcher;

import java.util.concurrent.ArrayBlockingQueue;

public class BlockingQueueDispatcherTest extends BaseDispatcherTest<BlockingQueueDispatcher> {

  @Override
  protected BlockingQueueDispatcher createDispatcher(final Dispatcher.ExceptionHandler exceptionHandler) {
    return new BlockingQueueDispatcher(new ArrayBlockingQueue<BlockingQueueDispatcher.Entry>(10), BlockingQueueDispatcher.defaultExecutorService(), 2, exceptionHandler);
  }

}