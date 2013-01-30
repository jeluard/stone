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
package com.github.jeluard.stone.dispatcher.sequential;

import com.github.jeluard.stone.api.ConsolidationListener;
import com.github.jeluard.stone.api.Consolidator;
import com.github.jeluard.stone.api.Window;
import com.github.jeluard.stone.spi.Dispatcher;
import com.github.jeluard.stone.spi.Storage;

import java.io.IOException;

/**
 * {@link Dispatcher} implementation executing {@link Dispatcher#accumulateAndPersist(com.github.jeluard.stone.api.Window, com.github.jeluard.stone.spi.Storage, com.github.jeluard.stone.api.Consolidator[], com.github.jeluard.stone.api.ConsolidationListener[], long, long, long, int)} in the caller thread.
 */
public class SequentialDispatcher extends Dispatcher {

  public SequentialDispatcher() {
    this(Dispatcher.DEFAULT_EXCEPTION_HANDLER);
  }

  public SequentialDispatcher(final ExceptionHandler exceptionHandler) {
    super(exceptionHandler);
  }

  @Override
  public final boolean dispatch(final Window window, final Storage storage, final Consolidator[] consolidators, final ConsolidationListener[] consolidationListeners, final long beginningTimestamp, final long previousTimestamp, final long currentTimestamp, final int value) throws IOException {
    try {
      accumulateAndPersist(window, storage, consolidators, consolidationListeners, beginningTimestamp, previousTimestamp, currentTimestamp, value);
    } catch (Exception e) {
      notifyExceptionHandler(e);
    }
    return true;
  }

}