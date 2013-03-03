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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.LinkedList;

import org.junit.Test;

public class ByteBufferStorageTest {

  @Test
  public void shouldDataBeAccessible() throws IOException {
    final ByteBufferStorage storage = new ByteBufferStorage(10) {
      @Override
      protected void append(final ByteBuffer buffer) throws IOException {
      }
      @Override
      public Iterable<Pair<Long, int[]>> all() throws IOException {
        return new LinkedList<Pair<Long, int[]>>();
      }
    };
    storage.append(System.currentTimeMillis(), new int[0]);
  }

}