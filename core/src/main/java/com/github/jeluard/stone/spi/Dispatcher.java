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

import com.github.jeluard.stone.api.Listener;
import com.github.jeluard.stone.helper.Loggers;
import com.google.common.base.Preconditions;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Encapsulate the logic that calls {@link Listener#onPublication(long, long, int)}.
 */
public abstract class Dispatcher {

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

  private final ExceptionHandler exceptionHandler;
  protected static final ExceptionHandler DEFAULT_EXCEPTION_HANDLER = new ExceptionHandler() {
    @Override
    public void onException(final Exception exception) {
      if (Dispatcher.LOGGER.isLoggable(Level.WARNING)) {
        Dispatcher.LOGGER.log(Level.WARNING, "Got exception while publishing", exception);
      }
    }
  };

  /**
   * @param exceptionHandler 
   */
  public Dispatcher(final ExceptionHandler exceptionHandler) {
    this.exceptionHandler = Preconditions.checkNotNull(exceptionHandler, "null exceptionHandler");
  }

  /**
   * Safely execute {@link ExceptionHandler#onException(java.lang.Exception)} 
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
   * Dispatch {@link Listener}s executions.
   *
   * @param previousTimestamp {@linkplain com.github.jeluard.stone.api.TimeSeries#DEFAULT_LATEST_TIMESTAMP} if does not exist
   * @param currentTimestamp
   * @param value
   * @param listeners
   * @return true if dispatch has been accepted; false if rejected
   */
   public abstract boolean dispatch(long previousTimestamp, long currentTimestamp, int value, Listener[] listeners);

}