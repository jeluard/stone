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
import com.github.jeluard.stone.api.Window;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Iterables;
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

  protected final Window window;
  private final long duration;
  private final int maximumSize;

  public Storage(final Window window) {
    this.window = window;
    this.duration = window.getPersistedDuration().get().getMillis();
    this.maximumSize = (int) (this.duration / window.getResolution().getMillis());
  }

  protected final long getDuration() {
    return this.duration;
  }

  protected final int getMaximumSize() {
    return this.maximumSize;
  }

  protected final Optional<DateTime> minimum() throws IOException {
    final Optional<DateTime> end = end();
    if (!end.isPresent()) {
      return Optional.absent();
    }

    return Optional.<DateTime>of(end.get().minus(this.duration));
  }

  protected final Optional<DateTime> maximum() throws IOException {
    final Optional<DateTime> beginning = beginning();
    if (!beginning.isPresent()) {
      return Optional.absent();
    }

    return Optional.<DateTime>of(beginning.get().plus(this.duration));
  }

  /**
   * Default implementation relying on {@link #all()}.
   *
   * @return 
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
   * @return 
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
   * @param interval 
   * @return 
   * @see AbstractIterator
   */
  @Override
  public Iterable<Pair<Long, int[]>> during(final Interval interval) throws IOException {
    Preconditions.checkNotNull(interval, "null interval");

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

  /**
   * @return currently number of contained elements. Default implementation relies on {@link #all()}
   * @throws IOException 
   */
  @Override
  public int size() throws IOException {
    return Iterables.size(all());
  }

  /**
   * @return true if {@code maxSize} elements are contained. Default implementation relies on {@link #size()}
   * @throws IOException 
   */
  protected boolean isFull() throws IOException {
    return size() == getMaximumSize();
  }

}