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

import com.github.jeluard.guayaba.test.junit.LoggerRule;
import com.github.jeluard.stone.api.Listener;

import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

public abstract class BaseDispatcherTest<T extends Dispatcher> {

  @Rule
  public LoggerRule loggerRule = new LoggerRule(Dispatcher.LOGGER);

  protected T createDispatcher() {
    return createDispatcher(Dispatcher.DEFAULT_EXCEPTION_HANDLER);
  }

  protected abstract T createDispatcher(Dispatcher.ExceptionHandler exceptionHandler);

  protected Listener createListener() {
    return new Listener() {
      @Override
      public void onPublication(long previousTimestamp, long currentTimestamp, int value) {
      }
    };
  }

  protected Listener createFailingListener() {
    return new Listener() {
      @Override
      public void onPublication(long previousTimestamp, long currentTimestamp, int value) {
        throw new RuntimeException();
      }
    };
  }

  @Test
  public void shouldCorrectValueBeDispatched() {
    final Dispatcher dispatcher = createDispatcher();
    final long now = System.currentTimeMillis();
    dispatcher.dispatch(now, now+1, 1, new Listener[]{createListener()});
  }

  @Test
  public void shouldCorrectValueBeDispatchedWithZeroListener() {
    final Dispatcher dispatcher = createDispatcher();
    final long now = System.currentTimeMillis();
    dispatcher.dispatch(now, now+1, 1, new Listener[0]);
  }

  @Test
  public void shouldListenerExceptionBeDelegated() {
    final Dispatcher.ExceptionHandler exceptionHandler = Mockito.mock(Dispatcher.ExceptionHandler.class);
    final Dispatcher dispatcher = createDispatcher(exceptionHandler);
    final long now = System.currentTimeMillis();
    dispatcher.dispatch(now, now+1, 1, new Listener[]{createFailingListener()});

    Mockito.verify(exceptionHandler).onException(Mockito.<Exception>any());
  }

  @Test
  public void shouldFailingExceptionListenerBeHandled() {
    final Dispatcher.ExceptionHandler exceptionHandler = Mockito.mock(Dispatcher.ExceptionHandler.class);
    Mockito.doThrow(new RuntimeException()).when(exceptionHandler).onException(Mockito.<Exception>any());
    final Dispatcher dispatcher = createDispatcher(exceptionHandler);
    final long now = System.currentTimeMillis();
    dispatcher.dispatch(now, now+1, 1, new Listener[]{createFailingListener()});
  }

}