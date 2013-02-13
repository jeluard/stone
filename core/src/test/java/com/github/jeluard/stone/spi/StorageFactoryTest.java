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

import com.github.jeluard.guayaba.base.Pair;
import com.github.jeluard.guayaba.test.junit.LoggerRule;
import com.github.jeluard.stone.api.Window;
import com.github.jeluard.stone.consolidator.MaxConsolidator;
import com.github.jeluard.stone.helper.Loggers;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import java.io.Closeable;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;

import org.joda.time.Duration;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

public class StorageFactoryTest {

  @Rule
  public final LoggerRule loggerRule = new LoggerRule(Loggers.BASE_LOGGER);

  private Window createWindow() {
    return Window.of(Duration.standardSeconds(1)).persistedDuring(Duration.standardSeconds(10)).consolidatedBy(MaxConsolidator.class);
  }

  private Storage createStorage() {
    return new Storage(createWindow()) {
      @Override
      public void onConsolidation(long timestamp, int[] data) throws IOException {
      }
      @Override
      public Iterable<Pair<Long, int[]>> all() throws IOException {
        return Collections.emptyList();
      }
    };
  }

  @Test
  public void shouldCreatedStorageBeAccessible() throws IOException {
    final StorageFactory storageFactory = new StorageFactory() {
      @Override
      protected Storage create(final String id, final Window window) throws IOException {
        return createStorage();
      }
    };

    final Storage storage = storageFactory.createOrGet("id", createWindow());

    Assert.assertTrue(Iterables.contains(storageFactory.getStorages(), storage));
  }

  @Test
  public void shouldCreatedStorageBeCached() throws IOException {
    final AtomicInteger count = new AtomicInteger();
    final StorageFactory storageFactory = new StorageFactory() {
      @Override
      protected Storage create(final String id, final Window window) throws IOException {
        count.incrementAndGet();
        return createStorage();
      }
    };

    final String id = "id";
    final Window window = createWindow();
    storageFactory.createOrGet(id, window);
    storageFactory.createOrGet(id, window);

    Assert.assertEquals(1, count.get());
  }

  @Test(expected=RuntimeException.class)
  public void shouldCreationFailureBePropagated() throws IOException {
    new StorageFactory() {
      @Override
      protected Storage create(final String id, final Window window) throws IOException {
        throw new IOException();
      }
    }.createOrGet("id", createWindow());
  }

  @Test
  public void shouldCloseReleaseCache() throws IOException {
    final AtomicInteger count = new AtomicInteger();
    final StorageFactory storageFactory = new StorageFactory() {
      @Override
      protected Storage create(final String id, final Window window) throws IOException {
        count.incrementAndGet();
        return createStorage();
      }
    };

    final String id = "id";
    final Window window = createWindow();
    storageFactory.createOrGet(id, window);
    storageFactory.close();
    storageFactory.createOrGet(id, window);

    Assert.assertEquals(2, count.get());
  }

  @Test
  public void shouldCloseDelegateCloseToStorage() throws IOException {
    final AtomicInteger count = new AtomicInteger();
    class FailingStorage extends Storage implements Closeable {
      public FailingStorage() {
        super(createWindow());
      }
      @Override
      public Iterable<Pair<Long, int[]>> all() throws IOException {
        throw new UnsupportedOperationException("Not supported yet.");
      }
      @Override
      public void onConsolidation(long timestamp, int[] consolidates) throws Exception {
      }
      @Override
      public void close() throws IOException {
        count.incrementAndGet();
      }
    }
    final StorageFactory storageFactory = new StorageFactory() {
      @Override
      protected Storage create(final String id, final Window window) throws IOException {
        return new FailingStorage();
      }
    };

    final String id = "id";
    final Window window = createWindow();
    storageFactory.createOrGet(id, window);
    storageFactory.close();

    Assert.assertEquals(1, count.get());
  }

  @Test
  public void shouldCleanupBeInvokedDuringClose() throws IOException {
    final AtomicInteger count = new AtomicInteger();
    final StorageFactory storageFactory = new StorageFactory() {
      @Override
      protected Storage create(final String id, final Window window) throws IOException {
        return createStorage();
      }
      @Override
      protected void cleanup() throws IOException {
        count.incrementAndGet();
      }
    };

    storageFactory.close();

    Assert.assertEquals(1, count.get());
  }

  @Test
  public void shouldClosingExistingStorageBeSuccessful() throws IOException {
    final StorageFactory storageFactory = new StorageFactory() {
      @Override
      protected Storage create(final String id, final Window window) throws IOException {
        return createStorage();
      }
    };

    final String id = "id";
    final Window window = createWindow();
    storageFactory.createOrGet(id, window);
    storageFactory.close(id, window);

    Assert.assertTrue(Iterables.isEmpty(storageFactory.getStorages()));
  }

  @Test
  public void shouldClosingNonExistingStorageBeSafe() throws IOException {
    final StorageFactory storageFactory = new StorageFactory() {
      @Override
      protected Storage create(final String id, final Window window) throws IOException {
        return createStorage();
      }
    };

    final Window window = createWindow();
    storageFactory.createOrGet("id", window);
    storageFactory.close("id2", window);

    Assert.assertFalse(Iterables.isEmpty(storageFactory.getStorages()));
  }

}