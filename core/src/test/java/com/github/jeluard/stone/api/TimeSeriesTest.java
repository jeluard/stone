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

import com.github.jeluard.guayaba.base.Pair;
import com.github.jeluard.guayaba.test.junit.LoggerRule;
import com.github.jeluard.stone.consolidator.MaxConsolidator;
import com.github.jeluard.stone.consolidator.Percentile90Consolidator;
import com.github.jeluard.stone.helper.Loggers;
import com.github.jeluard.stone.spi.Dispatcher;
import com.github.jeluard.stone.spi.Storage;
import com.github.jeluard.stone.spi.StorageFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;

import org.joda.time.Duration;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

public class TimeSeriesTest {

  static class ConsolidatorWithInvalidConstructor extends Consolidator {
    public ConsolidatorWithInvalidConstructor(float argument) {
    }
    @Override
    public void accumulate(long timestamp, int value) {
    }
    @Override
    protected int consolidate() {
      return 0;
    }
    @Override
    protected void reset() {
    }
  }

  static class ConsolidatorWithFailingConstructor extends Consolidator {
    public ConsolidatorWithFailingConstructor() {
      throw new RuntimeException();
    }
    @Override
    public void accumulate(long timestamp, int value) {
    }
    @Override
    protected int consolidate() {
      return 0;
    }

    @Override
    protected void reset() {
    }
  }

  @Rule
  public final LoggerRule loggerRule = new LoggerRule(Loggers.BASE_LOGGER);

  private Window createWindow() {
    return Window.of(Duration.standardSeconds(1)).persistedDuring(Duration.standardSeconds(10)).consolidatedBy(MaxConsolidator.class);
  }

  private StorageFactory createStorageFactory() {
    return new StorageFactory() {
      @Override
      protected Storage create(String id, Window window) throws IOException {
        return new Storage(createWindow()) {
          @Override
          public Iterable<Pair<Long, int[]>> all() throws IOException {
            return Collections.emptyList();
          }
          @Override
          public void onConsolidation(long timestamp, int[] consolidates) throws Exception {
          }
        };
      }
    };
  }

  private StorageFactory createFailingOnCloseStorage() {
    class CloseFailingStorage extends Storage implements Closeable {
      public CloseFailingStorage() {
        super(createWindow());
      }
      @Override
      public Iterable<Pair<Long, int[]>> all() throws IOException {
        return Collections.emptyList();
      }
      @Override
      public void onConsolidation(long timestamp, int[] consolidates) throws Exception {
      }
      @Override
      public void close() throws IOException {
        throw new IOException();
      }
    }
    return new StorageFactory() {
      @Override
      protected Storage create(String id, Window window) throws IOException {
        return new CloseFailingStorage();
      }
    };
  }

  @Test
  public void shouldCreationBeSuccessful() throws IOException {
    final TimeSeries timeSeries = new TimeSeries("id", Duration.millis(1), new Window[]{createWindow()}, Mockito.mock(Dispatcher.class), createStorageFactory());
    timeSeries.close();
  }

  @Test
  public void shouldCreationWithConsolidatorDefiningDefaultConstructorBeSuccessful() throws IOException {
    final Window window = Window.of(Duration.millis(1)).persistedDuring(Duration.millis(1)).consolidatedBy(MaxConsolidator.class);
    final TimeSeries timeSeries = new TimeSeries("id", Duration.millis(1), new Window[]{window}, Mockito.mock(Dispatcher.class), createStorageFactory());
    timeSeries.close();
  }

  @Test
  public void shouldCreationWithConsolidatorDefiningIntConstructorBeSuccessful() throws IOException {
    final Window window = Window.of(Duration.standardHours(1)).persistedDuring(Duration.standardDays(10000)).consolidatedBy(Percentile90Consolidator.class);
    final TimeSeries timeSeries = new TimeSeries("id", Duration.millis(1), new Window[]{window}, Mockito.mock(Dispatcher.class), createStorageFactory());
    timeSeries.close();

    final Window window2 = Window.of(Duration.millis(1)).persistedDuring(Duration.standardDays(10000)).consolidatedBy(Percentile90Consolidator.class);
    final TimeSeries timeSeries2 = new TimeSeries("id", Duration.millis(1), new Window[]{window2}, Mockito.mock(Dispatcher.class), createStorageFactory());
    timeSeries2.close();
  }

  @Test(expected=IllegalArgumentException.class)
  public void shouldCreationWithConsolidatorDefiningInvalidConstructorBeInvalid() throws IOException {
    final Window window = Window.of(Duration.millis(1)).persistedDuring(Duration.millis(1)).consolidatedBy(ConsolidatorWithInvalidConstructor.class);
    final TimeSeries timeSeries = new TimeSeries("id", Duration.millis(1), new Window[]{window}, Mockito.mock(Dispatcher.class), createStorageFactory());
    timeSeries.close();
  }

  @Test(expected=RuntimeException.class)
  public void shouldCreationWithConsolidatorDefiningFailingConstructorBeInvalid() throws IOException {
    final Window window = Window.of(Duration.millis(1)).persistedDuring(Duration.millis(1)).consolidatedBy(ConsolidatorWithFailingConstructor.class);
    final TimeSeries timeSeries = new TimeSeries("id", Duration.millis(1), new Window[]{window}, Mockito.mock(Dispatcher.class), createStorageFactory());
    timeSeries.close();
  }

  @Test
  public void shouldIDBeAccessible() throws IOException {
    final String id = "id";
    final TimeSeries timeSeries = new TimeSeries(id, Duration.millis(1), new Window[]{createWindow()}, Mockito.mock(Dispatcher.class), createStorageFactory());
    timeSeries.close();

    Assert.assertEquals(id, timeSeries.getId());
  }

  @Test
  public void shouldReadersBeEmptyWhenNoWindowIsPersisted() throws IOException {
    final Window window = Window.of(Duration.millis(1)).consolidatedBy(MaxConsolidator.class);
    final TimeSeries timeSeries = new TimeSeries("id", Duration.millis(1), new Window[]{window}, Mockito.mock(Dispatcher.class), createStorageFactory());
    timeSeries.close();

    Assert.assertTrue(timeSeries.getReaders().isEmpty());
  }

  @Test
  public void shouldReadersNotBeEmptyWhenSomeWindowIsPersisted() throws IOException {
    final Window window = Window.of(Duration.millis(1)).persistedDuring(Duration.millis(1)).consolidatedBy(MaxConsolidator.class);
    final TimeSeries timeSeries = new TimeSeries("id", Duration.millis(1), new Window[]{window}, Mockito.mock(Dispatcher.class), createStorageFactory());
    timeSeries.close();

    Assert.assertFalse(timeSeries.getReaders().isEmpty());
  }

  @Test
  public void shouldReadersBeEmptyWhenSomeWindowContainsConsolidationListener() throws IOException {
    final Window window = Window.of(Duration.millis(1)).listenedBy(new ConsolidationListener() {
      @Override
      public void onConsolidation(long timestamp, int[] consolidates) throws Exception {
      }
    }).consolidatedBy(MaxConsolidator.class);
    final TimeSeries timeSeries = new TimeSeries("id", Duration.millis(1), new Window[]{window}, Mockito.mock(Dispatcher.class), createStorageFactory());
    timeSeries.close();

    Assert.assertTrue(timeSeries.getReaders().isEmpty());
  }

  @Test
  public void shouldCloseBeIdempotent() throws IOException {
    final TimeSeries timeSeries = new TimeSeries("id", Duration.millis(1), new Window[]{createWindow()}, Mockito.mock(Dispatcher.class), createStorageFactory());
    timeSeries.close();
    timeSeries.close();
  }

  @Test
  public void shouldCloseWithFailingStorageBeHandled() throws IOException {
    final Window window = Window.of(Duration.millis(1)).persistedDuring(Duration.millis(1)).consolidatedBy(MaxConsolidator.class);
    final TimeSeries timeSeries = new TimeSeries("id", Duration.millis(1), new Window[]{window}, Mockito.mock(Dispatcher.class), createFailingOnCloseStorage());
    timeSeries.close();
  }

  @Test
  public void shouldDeleteBeIdempotent() throws IOException {
    final TimeSeries timeSeries = new TimeSeries("id", Duration.millis(1), new Window[]{createWindow()}, Mockito.mock(Dispatcher.class), createStorageFactory());
    timeSeries.close();
    timeSeries.delete();
    timeSeries.delete();
  }

  @Test(expected=IllegalArgumentException.class)
  public void shouldCreationWithEmptyWindowBeInvalid() throws IOException {
    new TimeSeries("id", Duration.millis(1), new Window[0], Mockito.mock(Dispatcher.class), createStorageFactory());
  }

  @Test(expected=IllegalArgumentException.class)
  public void shouldCreationWithDuplicatedIDBeInvalid() throws IOException {
    final String id = "id";
    final TimeSeries timeSeries = new TimeSeries(id, Duration.millis(1), new Window[]{createWindow()}, Mockito.mock(Dispatcher.class), createStorageFactory());
    try {
      new TimeSeries(id, Duration.millis(1), new Window[]{createWindow()}, Mockito.mock(Dispatcher.class), createStorageFactory());
    } finally {
      timeSeries.close();
    }
  }

  @Test
  public void shouldCreationWithDuplicatedIDBeSuccessfulAfterClose() throws IOException {
    final String id = "id";
    final TimeSeries timeSeries = new TimeSeries(id, Duration.millis(1), new Window[]{createWindow()}, Mockito.mock(Dispatcher.class), createStorageFactory());
    timeSeries.close();
    final TimeSeries timeSeries2 = new TimeSeries(id, Duration.millis(1), new Window[]{createWindow()}, Mockito.mock(Dispatcher.class), createStorageFactory());
    timeSeries2.close(); 
  }

  @Test
  public void shouldPublicationBeSuccessful() throws IOException {
    final TimeSeries timeSeries = new TimeSeries("id", Duration.millis(1), new Window[]{createWindow()}, Mockito.mock(Dispatcher.class), createStorageFactory());
    timeSeries.publish(1, 1);
    timeSeries.close();
  }

  @Test
  public void shouldMonotonicPublicationBeSuccessful() throws IOException {
    final TimeSeries timeSeries = new TimeSeries("id", Duration.millis(1), new Window[]{createWindow()}, Mockito.mock(Dispatcher.class), createStorageFactory());
    timeSeries.publish(1, 1);
    timeSeries.publish(2, 1);
    timeSeries.close();
  }

  @Test
  public void shouldZeroAsTimestampBeRejected() throws IOException {
    final TimeSeries timeSeries = new TimeSeries("id", Duration.millis(1), new Window[]{createWindow()}, Mockito.mock(Dispatcher.class), createStorageFactory());
    Assert.assertFalse(timeSeries.publish(0, 0));
    timeSeries.close();
  }

  @Test
  public void shouldTwiceSameTimestampBeRejected() throws IOException {
    final TimeSeries timeSeries = new TimeSeries("id", Duration.millis(1), new Window[]{createWindow()}, Mockito.mock(Dispatcher.class), createStorageFactory());
    final long now = System.currentTimeMillis();
    Assert.assertTrue(timeSeries.publish(now, 0));
    Assert.assertFalse(timeSeries.publish(now, 0));
    timeSeries.close();
  }

  /*@Test
  public void shouldTimestampEqualToStorageLastBeRejected() throws IOException {
    final long now = System.currentTimeMillis();
    final TimeSeries timeSeries = new TimeSeries("id", Duration.standardSeconds(2), new Window[0], Mockito.mock(Dispatcher.class), createStorageMock(Optional.of(DateTime.now())));
    Assert.assertFalse(timeSeries.publish(now, 0));
    timeSeries.close();
  }*/

}