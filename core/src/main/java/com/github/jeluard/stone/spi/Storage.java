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
import com.github.jeluard.stone.api.ConsolidationListener;
import com.github.jeluard.stone.api.Reader;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Iterators;

import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;

import javax.annotation.concurrent.ThreadSafe;

import org.joda.time.DateTime;
import org.joda.time.Interval;

/**
 * Abstraction dealing with {@link TimeSeries} persistency.
 * <br>
 * A {@link Storage} is specific to a single {@link Window} of a {@link TimeSeries}.
 */
@ThreadSafe
public abstract class Storage implements Reader, ConsolidationListener {

  /**
   * Default implementation relying on {@link #all()}.
   *
   * {@inheritDoc}
   */
  @Override
  public Optional<DateTime> beginning() throws IOException {
    try {
      final Iterator<Pair<Long, int[]>> consolidates = all().iterator();
      return Optional.of(new DateTime(consolidates.next().first));
    } catch (NoSuchElementException e) {
      return Optional.absent();
    }
  }

  /**
   * Default implementation relying on {@link #all()}: it iterates over {@link Storage#all()} elements to access the last one.
   *
   * {@inheritDoc}
   *
   * @see Iterables#getLast(java.lang.Iterable)
   */
  @Override
  public Optional<DateTime> end() throws IOException {
    try {
      final Iterator<Pair<Long, int[]>> consolidates = all().iterator();
      return Optional.of(new DateTime(Iterators.getLast(consolidates).first));
    } catch (NoSuchElementException e) {
      return Optional.absent();
    }
  }

  /**
   * Default implementation relying on {@link #all()}: it iterates over all elements while they are parts of specified `beginning`.
   *
   * {@inheritDoc}
   *
   * @see AbstractIterator
   */
  @Override
  public Iterable<Pair<Long, int[]>> during(final Interval interval) throws IOException {
    Preconditions.checkNotNull(interval, "null interval");

    final Iterator<Pair<Long, int[]>> all = all().iterator();
    return new Iterable<Pair<Long, int[]>>() {
      @Override 
      public Iterator<Pair<Long, int[]>> iterator() {
        return new AbstractIterator<Pair<Long, int[]>>() {
          @Override
          protected Pair<Long, int[]> computeNext() {
            while (all.hasNext()) {
              final Pair<Long, int[]> consolidates = all.next();
              final long timestamp = consolidates.first;
              if (timestamp < interval.getStartMillis()) {
                //Before the beginning
                continue;
              }
              if (timestamp > interval.getEndMillis()) {
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

}