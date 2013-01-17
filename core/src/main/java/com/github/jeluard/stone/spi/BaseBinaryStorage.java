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

/**
 */
public abstract class BaseBinaryStorage extends BaseStorage {

  /**
   * Put {@code timestamp} into {@code buffer}.
   *
   * @param ints
   * @param buffer 
   */
  protected final void put(final long timestamp, final ByteBuffer buffer) {
    buffer.putLong(timestamp);
  }

  /**
   * @param buffer
   * @return a {@link Long} composed of {@code buffer} content 
   */
  protected final long getTimestamp(final byte[] buffer) {
    assert buffer.length > 8;
    return ByteBuffer.wrap(buffer).getLong();
  }

  /**
   * Put all {@code ints} into {@code buffer}.
   *
   * @param ints
   * @param buffer 
   */
  protected final void put(final int[] ints, final ByteBuffer buffer) {
    for (final int i : ints) {
      buffer.putInt(i);
    }
  }

  /**
   * @param buffer
   * @return all {@link int[]} composed of {@code buffer} content 
   */
  protected final int[] getConsolidates(final byte[] buffer) {
    final int count = buffer.length/4;
    final int[] ints = new int[count];
    final ByteBuffer byteBuffer = ByteBuffer.wrap(buffer);
    for (int i = 0; i < count; i++) {
      ints[i] = byteBuffer.getInt();
    }
    return ints;
  }

  /**
   * @param bits
   * @return convert number of bits into number of bytes
   */
  protected final int bits2Bytes(final int bits) {
    return bits / Byte.SIZE;
  }

  /**
   * @param capacity
   * @return a {@link ByteBuffer} with {@code capacity} capacity
   */
  protected ByteBuffer createByteBuffer(final int capacity) {
    return ByteBuffer.allocate(capacity);
  }

  @Override
  public final void append(final long timestamp, final int[] consolidates) throws IOException {
    final int capacity = bits2Bytes(Long.SIZE) + bits2Bytes(Integer.SIZE) * consolidates.length;
    final ByteBuffer buffer = createByteBuffer(capacity);
    put(timestamp, buffer);
    put(consolidates, buffer);

    append(buffer);
  }

  /**
   * Append the content of this {@link ByteBuffer} to the {@link Storage}.
   *
   * @param buffer
   * @throws IOException 
   */
  protected abstract void append(ByteBuffer buffer) throws IOException;

}