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
package com.github.jeluard.stone.storage.chronicle;

import com.github.jeluard.stone.api.Window;
import com.github.jeluard.stone.helper.Loggers;
import com.github.jeluard.stone.spi.StorageFactory;
import com.google.common.base.Preconditions;
import com.higherfrequencytrading.chronicle.Chronicle;
import com.higherfrequencytrading.chronicle.impl.IntIndexedChronicle;

import java.io.File;
import java.io.IOException;
import java.util.logging.Logger;

/**
 * {@link StorageFactory} implementation creating {@link ChronicleStorage}.
 */
public class ChronicleStorageFactory extends StorageFactory<ChronicleStorage> {

  static final Logger LOGGER = Loggers.create("storage.chronicle");

  private static final String BASE_PATH_PREFIX = "chronicle-";

  protected String createBasePath(final String id) {
    return ChronicleStorageFactory.BASE_PATH_PREFIX+id;
  }

  protected Chronicle createChronicle(final String basePath) throws IOException {
    return new IntIndexedChronicle(basePath);
  }

  @Override
  public final ChronicleStorage create(final String id, final Window window) throws IOException {
    Preconditions.checkNotNull(id, "null id");
    Preconditions.checkNotNull(window, "null window");

    return new ChronicleStorage(window, createChronicle(createBasePath(id)));
  }

  @Override
  public void delete(final String id) throws IOException {
    final String basePath = createBasePath(id);
    new File(basePath+".index").delete();
    new File(basePath+".data").delete();
  }

}