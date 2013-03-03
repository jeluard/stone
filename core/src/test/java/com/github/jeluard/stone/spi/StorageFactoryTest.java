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
import com.github.jeluard.stone.helper.Loggers;
import com.google.common.collect.Iterables;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

public class StorageFactoryTest {

  @Rule
  public final LoggerRule loggerRule = new LoggerRule(Loggers.BASE_LOGGER);

  private Storage createStorage() {
    return new Storage(1) {
      @Override
      public void append(long timestamp, int[] data) throws IOException {
      }
      @Override
      public Iterable<Pair<Long, int[]>> all() throws IOException {
        return Collections.emptyList();
      }
    };
  }


  @Test(expected=IllegalArgumentException.class)
  public void shouldInvalidDurationBeRejected() throws IOException {
    final StorageFactory storageFactory = new StorageFactory() {
      @Override
      protected Storage create(final String id, final int maximumSize) throws IOException {
        return createStorage();
      }
    };

    storageFactory.createOrGet("id", 2, 3);
  }

  @Test
  public void shouldCreatedStorageBeAccessible() throws IOException {
    final StorageFactory storageFactory = new StorageFactory() {
      @Override
      protected Storage create(final String id, final int maximumSize) throws IOException {
        return createStorage();
      }
    };

    final Storage storage = storageFactory.createOrGet("id", 1, 1);

    Assert.assertTrue(Iterables.contains(storageFactory.getStorages(), storage));
  }

  @Test
  public void shouldCreatedStorageBeCached() throws IOException {
    final AtomicInteger count = new AtomicInteger();
    final StorageFactory storageFactory = new StorageFactory() {
      @Override
      protected Storage create(final String id, final int maximumSize) throws IOException {
        count.incrementAndGet();
        return createStorage();
      }
    };

    final String id = "id";
    storageFactory.createOrGet(id, 1, 1);
    storageFactory.createOrGet(id, 1, 1);

    Assert.assertEquals(1, count.get());
  }

  @Test(expected=RuntimeException.class)
  public void shouldCreationFailureBePropagated() throws IOException {
    new StorageFactory() {
      @Override
      protected Storage create(final String id, final int maximumSize) throws IOException {
        throw new IOException();
      }
    }.createOrGet("id", 1, 1);
  }

  @Test
  public void shouldCloseReleaseCache() throws IOException {
    final AtomicInteger count = new AtomicInteger();
    final StorageFactory storageFactory = new StorageFactory() {
      @Override
      protected Storage create(final String id, final int maximumSize) throws IOException {
        count.incrementAndGet();
        return createStorage();
      }
    };

    final String id = "id";
    storageFactory.createOrGet(id, 1, 1);
    storageFactory.close();
    storageFactory.createOrGet(id, 1, 1);

    Assert.assertEquals(2, count.get());
  }

  @Test
  public void shouldCloseDelegateCloseToStorage() throws IOException {
    final AtomicInteger count = new AtomicInteger();
    class InstrumentedStorage extends Storage implements Closeable {
      public InstrumentedStorage() {
        super(1);
      }
      @Override
      public Iterable<Pair<Long, int[]>> all() throws IOException {
        throw new UnsupportedOperationException("Not supported yet.");
      }
      @Override
      public void append(long timestamp, int[] consolidates) throws IOException {
      }
      @Override
      public void close() throws IOException {
        count.incrementAndGet();
      }
    }
    final StorageFactory storageFactory = new StorageFactory() {
      @Override
      protected Storage create(final String id, final int maximumSize) throws IOException {
        return new InstrumentedStorage();
      }
    };

    final String id = "id";
    storageFactory.createOrGet(id, 1, 1);
    storageFactory.close();

    Assert.assertEquals(1, count.get());
  }

  @Test
  public void shouldFailingCloseBeHandled() throws IOException {
    class FailingStorage extends Storage implements Closeable {
      public FailingStorage() {
        super(1);
      }
      @Override
      public Iterable<Pair<Long, int[]>> all() throws IOException {
        throw new UnsupportedOperationException("Not supported yet.");
      }
      @Override
      public void append(long timestamp, int[] consolidates) throws IOException {
      }
      @Override
      public void close() throws IOException {
        throw new IOException();
      }
    }
    final StorageFactory storageFactory = new StorageFactory() {
      @Override
      protected Storage create(final String id, final int maximumSize) throws IOException {
        return new FailingStorage();
      }
    };

    final String id = "id";
    storageFactory.createOrGet(id, 1, 1);
    storageFactory.close();
  }

  @Test
  public void shouldCleanupBeInvokedDuringClose() throws IOException {
    final AtomicInteger count = new AtomicInteger();
    final StorageFactory storageFactory = new StorageFactory() {
      @Override
      protected Storage create(final String id, final int maximumSize) throws IOException {
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
      protected Storage create(final String id, final int maximumSize) throws IOException {
        return createStorage();
      }
    };

    final String id = "id";
    storageFactory.createOrGet(id, 1, 1);
    storageFactory.close(id);

    Assert.assertTrue(Iterables.isEmpty(storageFactory.getStorages()));
  }

  @Test
  public void shouldClosingNonExistingStorageBeSafe() throws IOException {
    final StorageFactory storageFactory = new StorageFactory() {
      @Override
      protected Storage create(final String id, final int maximumSize) throws IOException {
        return createStorage();
      }
    };

    storageFactory.createOrGet("id", 1, 1);
    storageFactory.close("id2");

    Assert.assertFalse(Iterables.isEmpty(storageFactory.getStorages()));
  }

  @Test
  public void shouldDeletingNonExistingStorageBeSafe() throws IOException {
    final StorageFactory storageFactory = new StorageFactory() {
      @Override
      protected Storage create(final String id, final int maximumSize) throws IOException {
        return createStorage();
      }
    };

    storageFactory.delete("id");
  }

}