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

import com.github.jeluard.stone.api.DataAggregates;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Iterables;

import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.joda.time.DateTime;
import org.joda.time.Interval;

/**
 * Base implementation for {@link Storage}.
 */
public abstract class BaseStorage implements Storage {

  /**
   * Default implementation relying on {@link #all()}: it iterates over all elements to access the last one.
   *
   * {@inheritDoc}
   *
   * @see Iterables#getLast(java.lang.Iterable)
   */
  @Override
  public Optional<DateTime> last() throws IOException {
    try {
      return Optional.of(Iterables.getLast(all()).getDate());
    } catch (NoSuchElementException e) {
      return Optional.absent();
    }
  }

  /**
   * Default implementation relying on {@link #all()}: it iterates over all elements while they are parts of specified `interval`.
   *
   * {@inheritDoc}
   *
   * @see AbstractIterator
   */
  @Override
  public Iterable<DataAggregates> during(final Interval interval) throws IOException {
    Preconditions.checkNotNull(interval, "null interval");

    final Iterator<DataAggregates> all = all().iterator();
    return new Iterable<DataAggregates>() {
      @Override 
      public Iterator<DataAggregates> iterator() {
        return new AbstractIterator<DataAggregates>() {
          @Override
          protected DataAggregates computeNext() {
            while (all.hasNext()) {
              final DataAggregates aggregates = all.next();
              final DateTime date = aggregates.getDate();
              if (!interval.contains(date)) {
                if (interval.isBefore(date)) {
                  break;
                } else {
                  continue;
                }
              }
              return aggregates;
            }
            return endOfData();
          }
        };
      }
    };
  }

}