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

import com.github.jeluard.stone.api.Window;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Base implementation for {@link Storage} storing byte arrays.
 */
public abstract class ByteBufferStorage extends Storage {

  public ByteBufferStorage(final Window window) {
    super(window);
  }

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
   * @return a {@link Long} composed of first 8 bytes of {@code buffer} content. Next bytes are ignored.
   */
  protected final long getTimestamp(final byte[] buffer) {
    assert buffer.length > bits2Bytes(Long.SIZE);
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
   * @param offset number of bytes to skip
   * @return all {@link int[]} composed of {@code buffer} content 
   */
  protected final int[] getConsolidates(final byte[] buffer, final int offset) {
    assert (buffer.length - offset) % bits2Bytes(Integer.SIZE) == 0;
    final int count = (buffer.length - offset) / bits2Bytes(Integer.SIZE);
    final int[] ints = new int[count];
    final ByteBuffer byteBuffer = ByteBuffer.wrap(buffer, offset, buffer.length - offset);
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
  public final void onConsolidation(final long timestamp, final int[] consolidates) throws IOException {
    final int capacity = bits2Bytes(Long.SIZE) + bits2Bytes(Integer.SIZE) * consolidates.length;
    final ByteBuffer buffer = createByteBuffer(capacity);
    put(timestamp, buffer);
    put(consolidates, buffer);

    append(timestamp, buffer);
  }

  /**
   * Append the content of this {@link ByteBuffer} to the {@link Storage}.
   *
   * @param timestamp
   * @param buffer
   * @throws IOException 
   */
  protected abstract void append(long timestamp, ByteBuffer buffer) throws IOException;

}