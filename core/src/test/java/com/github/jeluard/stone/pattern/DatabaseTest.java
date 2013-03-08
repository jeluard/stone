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
package com.github.jeluard.stone.pattern;

import com.github.jeluard.stone.api.TimeSeries;
import com.github.jeluard.stone.api.Window;
import com.github.jeluard.stone.api.WindowedTimeSeries;
import com.github.jeluard.stone.consolidator.MaxConsolidator;
import com.github.jeluard.stone.spi.Dispatcher;
import com.github.jeluard.stone.spi.Storage;
import com.github.jeluard.stone.spi.StorageFactory;
import com.google.common.base.Optional;

import java.io.IOException;
import org.junit.Assert;

import org.junit.Test;
import org.mockito.Mockito;

public class DatabaseTest {

  protected WindowedTimeSeries create(final Database database, final String id) throws IOException {
    return database.createOrOpen(id, 1, Window.of(2).consolidatedBy(MaxConsolidator.class));
  }

  protected StorageFactory createStorageFactory() throws IOException {
    final Storage storage = Mockito.mock(Storage.class);
    Mockito.when(storage.end()).thenReturn(Optional.<Long>absent());
    return createStorageFactory(storage);
  }

  protected StorageFactory createStorageFactory(final Storage storage) {
    return new StorageFactory() {
      @Override
      protected Storage create(final String id, final int maximumSize) throws IOException {
        return storage;
      }
    };
  }

  @Test
  public void shouldCreatedTSBeRemovable() throws IOException {
    final Database database = new Database(Mockito.mock(Dispatcher.class), createStorageFactory());
    final String id = "id";

    create(database, id);
    Assert.assertTrue(database.close(id));

    create(database, id);
    Assert.assertTrue(database.close(id));
  }

  @Test
  public void shouldRemoveBeIdempotent() throws IOException {
    final Database database = new Database(Mockito.mock(Dispatcher.class), createStorageFactory());
    final String id = "id";
    create(database, id);
    Assert.assertTrue(database.close(id));
    Assert.assertFalse(database.close(id));
  }

  @Test
  public void shouldClosedTSNotBeRemovable() throws IOException {
    final Database database = new Database(Mockito.mock(Dispatcher.class), createStorageFactory());
    final String id = "id";
    final TimeSeries timeSeries = create(database, id);
    timeSeries.close();

    Assert.assertFalse(database.close(id));

    database.createOrOpen(id, 1);
    Assert.assertTrue(database.close(id));
  }

  @Test
  public void shouldCloseRemoveEverything() throws IOException {
    final Database database = new Database(Mockito.mock(Dispatcher.class), createStorageFactory());
    final String id = "id";
    create(database, id);
    database.close();

    Assert.assertFalse(database.close(id));

    create(database, id);
    Assert.assertTrue(database.close(id));
  }

}