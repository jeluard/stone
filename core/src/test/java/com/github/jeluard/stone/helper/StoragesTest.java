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
package com.github.jeluard.stone.helper;

import com.github.jeluard.guayaba.test.AbstractHelperClassTest;
import com.github.jeluard.guayaba.test.junit.LoggerRule;
import com.github.jeluard.stone.api.ConsolidationListener;
import com.github.jeluard.stone.spi.Storage;

import java.io.IOException;

import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

public class StoragesTest extends AbstractHelperClassTest {

  @Rule
  public LoggerRule loggerRule = new LoggerRule(Loggers.BASE_LOGGER);

  @Override
  protected Class<?> getType() {
    return Storages.class;
  }

  @Test
  public void shouldConsolidationBePropagated() throws IOException {
    final Storage storage = Mockito.mock(Storage.class);
    final ConsolidationListener consolidationListener = Storages.asConsolidationListener(storage, Loggers.BASE_LOGGER);
    consolidationListener.onConsolidation(System.currentTimeMillis(), new int[0]);

    Mockito.verify(storage).append(Mockito.anyLong(), Mockito.any(int[].class));
  }

  @Test
  public void shouldConsolidationListenerBeSafe() throws IOException {
    final Storage storage = Mockito.mock(Storage.class);
    Mockito.doThrow(new IOException()).when(storage).append(Mockito.anyLong(), Mockito.any(int[].class));
    final ConsolidationListener consolidationListener = Storages.asConsolidationListener(storage, Loggers.BASE_LOGGER);
    consolidationListener.onConsolidation(System.currentTimeMillis(), new int[0]);
  }

  @Test
  public void shouldLatestTimestampDelegateToEnd() throws IOException {
    final Storage storage = Mockito.mock(Storage.class);
    final ConsolidationListener.Persistent consolidationListener = Storages.asConsolidationListener(storage, Loggers.BASE_LOGGER);
    consolidationListener.getLatestTimestamp();

    Mockito.verify(storage).end();
  }

  @Test(expected =RuntimeException.class)
  public void shouldEndFailureBePropagated() throws IOException {
    final Storage storage = Mockito.mock(Storage.class);
    Mockito.doThrow(new IOException()).when(storage).end();
    final ConsolidationListener.Persistent consolidationListener = Storages.asConsolidationListener(storage, Loggers.BASE_LOGGER);
    consolidationListener.getLatestTimestamp();
  }

}