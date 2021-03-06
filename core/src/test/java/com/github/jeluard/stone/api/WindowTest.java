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
package com.github.jeluard.stone.api;

import com.github.jeluard.guayaba.test.AbstractTest;
import com.github.jeluard.guayaba.test.junit.LoggerRule;
import com.github.jeluard.stone.consolidator.MaxConsolidator;
import com.github.jeluard.stone.consolidator.MinConsolidator;
import com.github.jeluard.stone.helper.Loggers;

import java.util.Collections;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

public class WindowTest extends AbstractTest<Window> {

  private static final int DURATION_TWO = 2;
  private static final int DURATION_THREE = 3;

  @Rule
  public LoggerRule loggerRule = new LoggerRule(Loggers.BASE_LOGGER);

  @Override
  protected Class<Window> getType() {
    return Window.class;
  }

  @Override
  protected Window createInstance() throws Exception {
    return Window.of(WindowTest.DURATION_TWO).consolidatedBy(MaxConsolidator.class);
  }

  @Test(expected=IllegalArgumentException.class)
  public void shouldNegativeSizeBeInvalid() {
    Window.of(-1).consolidatedBy(MaxConsolidator.class);
  }

  @Test(expected=IllegalArgumentException.class)
  public void shouldTooSmallSizeBeInvalid() {
    Window.of(1).consolidatedBy(MaxConsolidator.class);
  }

  @Test
  public void shouldWindowBeCorrectlyCreated() {
    Assert.assertEquals(WindowTest.DURATION_TWO, Window.of(WindowTest.DURATION_TWO).consolidatedBy(MaxConsolidator.class).getSize());
    Assert.assertEquals(Collections.singletonList(MaxConsolidator.class), Window.of(WindowTest.DURATION_TWO).consolidatedBy(MaxConsolidator.class).getConsolidatorTypes());
  }

  @Test
  public void shouldListenerBeAccessibleIfProvided() {
    Assert.assertFalse (Window.of(WindowTest.DURATION_TWO).listenedBy(Mockito.mock(ConsolidationListener.class)).consolidatedBy(MaxConsolidator.class).getConsolidationListeners().isEmpty());
  }

  @Test
  public void shouldEqualsRelyOnAllArgument() {
    Assert.assertFalse(Window.of(WindowTest.DURATION_TWO).consolidatedBy(MaxConsolidator.class).equals(Window.of(WindowTest.DURATION_THREE).consolidatedBy(MaxConsolidator.class)));
    Assert.assertFalse(Window.of(WindowTest.DURATION_TWO).consolidatedBy(MaxConsolidator.class).equals(Window.of(WindowTest.DURATION_TWO).consolidatedBy(MinConsolidator.class)));
    Assert.assertFalse(Window.of(WindowTest.DURATION_TWO).listenedBy(Mockito.mock(ConsolidationListener.class)).consolidatedBy(MaxConsolidator.class).equals(Window.of(WindowTest.DURATION_TWO).consolidatedBy(MaxConsolidator.class)));
  }

}