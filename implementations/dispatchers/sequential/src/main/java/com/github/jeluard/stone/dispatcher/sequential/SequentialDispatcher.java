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

import com.github.jeluard.stone.api.Listener;
import com.github.jeluard.stone.spi.Dispatcher;

/**
 * {@link Dispatcher} implementation executing {@link Listener} one by one using caller thread.
 */
public class SequentialDispatcher extends Dispatcher {

  public SequentialDispatcher() {
    this(Dispatcher.DEFAULT_EXCEPTION_HANDLER);
  }

  public SequentialDispatcher(final ExceptionHandler exceptionHandler) {
    super(exceptionHandler);
  }

  @Override
  public boolean dispatch(final long previousTimestamp, final long currentTimestamp, final int value, final Listener[] listeners) {
    for (final Listener listener : listeners) {
      try {
        listener.onPublication(previousTimestamp, currentTimestamp, value);
      } catch (Exception e) {
        notifyExceptionHandler(e);
      }
    }
    return true;
  }

}