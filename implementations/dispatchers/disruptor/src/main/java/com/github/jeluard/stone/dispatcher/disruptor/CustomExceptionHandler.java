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

import com.lmax.disruptor.ExceptionHandler;

import java.util.logging.Level;

/**
 * An {@link ExceptionHandler} that allows extension.
 */
abstract class CustomExceptionHandler implements ExceptionHandler {

  @Override
  public void handleEventException(final Throwable e, final long sequence, final Object event) {
    if (DisruptorDispatcher.LOGGER.isLoggable(Level.WARNING)) {
      DisruptorDispatcher.LOGGER.log(Level.WARNING, "Got exception while processing event <"+event+">", e);
    }
  }

  @Override
  public void handleOnStartException(final Throwable e) {
    if (DisruptorDispatcher.LOGGER.isLoggable(Level.WARNING)) {
      DisruptorDispatcher.LOGGER.log(Level.WARNING, "Got exception during onStart", e);
    }
  }

  @Override
  public void handleOnShutdownException(final Throwable e) {
    if (DisruptorDispatcher.LOGGER.isLoggable(Level.WARNING)) {
      DisruptorDispatcher.LOGGER.log(Level.WARNING, "Got exception during onShutdown", e);
    }
  }

}