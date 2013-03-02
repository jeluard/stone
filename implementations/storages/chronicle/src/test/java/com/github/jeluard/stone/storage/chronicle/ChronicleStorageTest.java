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

import com.github.jeluard.stone.spi.BaseStorageTest;
import com.higherfrequencytrading.chronicle.impl.IndexedChronicle;

import java.io.File;
import java.io.IOException;

import org.junit.After;

public class ChronicleStorageTest extends BaseStorageTest<ChronicleStorage> {

  private static final String BASE_PATH = "chronicle";

  @After
  public void cleanupFolder() {
    new File(ChronicleStorageTest.BASE_PATH+".index").delete();
    new File(ChronicleStorageTest.BASE_PATH+".data").delete();
  }

  @Override
  protected ChronicleStorage createStorage(final int maximumSize) throws IOException {
    return new ChronicleStorage(maximumSize, new IndexedChronicle(ChronicleStorageTest.BASE_PATH));
  }

}