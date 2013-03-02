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
import com.github.jeluard.stone.api.Reader;
import com.google.common.base.Optional;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Iterators;

import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Abstraction dealing with {@link TimeSeries} persistency.
 */
@ThreadSafe
public abstract class Storage implements Reader {

  private final int maximumSize;

  public Storage(final int maximumSize) {
    this.maximumSize = maximumSize;
  }

  /**
   * @return
   */
  protected final int getMaximumSize() {
    return this.maximumSize;
  }

  /**
   * Default implementation relying on {@link #all()}.
   *
   * @return 
   */
  @Override
  public Optional<Long> beginning() throws IOException {
    try {
      final Iterator<Pair<Long, int[]>> consolidates = all().iterator();
      return Optional.of(consolidates.next().first);
    } catch (NoSuchElementException e) {
      return Optional.absent();
    }
  }

  /**
   * Default implementation relying on {@link #all()}: it iterates over {@link #all()} elements to access the last one.
   *
   * @return 
   * @see Iterables#getLast(java.lang.Iterable)
   */
  @Override
  public Optional<Long> end() throws IOException {
    try {
      final Iterator<Pair<Long, int[]>> consolidates = all().iterator();
      return Optional.of(Iterators.getLast(consolidates).first);
    } catch (NoSuchElementException e) {
      return Optional.absent();
    }
  }

  /**
   * Default implementation relying on {@link #all()}: it iterates over all elements while they are parts of specified {@code interval}.
   *
   * @param beginning
   * @param end
   * @return 
   * @see AbstractIterator
   */
  @Override
  public Iterable<Pair<Long, int[]>> during(final long beginning, final long end) throws IOException {
    return new Iterable<Pair<Long, int[]>>() {
      final Iterable<Pair<Long, int[]>> all = all();
      @Override 
      public Iterator<Pair<Long, int[]>> iterator() {
        return new AbstractIterator<Pair<Long, int[]>>() {
          final Iterator<Pair<Long, int[]>> iterator = all.iterator();
          @Override
          protected Pair<Long, int[]> computeNext() {
            while (this.iterator.hasNext()) {
              final Pair<Long, int[]> consolidates = this.iterator.next();
              final long timestamp = consolidates.first;
              if (timestamp < beginning) {
                //Before the beginning
                continue;
              }
              if (timestamp > end) {
                //After the beginning
                break;
              }

              return consolidates;
            }
            return endOfData();
          }
        };
      }
    };
  }

  /**
   * Append {@code values} associated to {@code timestamp}.
   *
   * @param timestamp
   * @param values
   * @throws IOException 
   */
  public abstract void append(long timestamp, int[] values) throws IOException;

}