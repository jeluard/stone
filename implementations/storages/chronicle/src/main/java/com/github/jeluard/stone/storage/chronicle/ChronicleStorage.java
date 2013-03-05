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

import com.github.jeluard.guayaba.base.Pair;
import com.github.jeluard.stone.spi.Storage;
import com.google.common.base.Preconditions;
import com.google.common.collect.AbstractIterator;
import com.higherfrequencytrading.chronicle.Chronicle;
import com.higherfrequencytrading.chronicle.Excerpt;

import java.io.IOException;
import java.util.Iterator;

/**
 * A {@link Storage} implementation relying on <a href="https://github.com/peter-lawrey/Java-Chronicle">Java Chronicle</a>.
 *
 * WARNING this implementation is not complete: it does not keep a constant size (nothing ever removed) and does not reorder data.
 */
public final class ChronicleStorage extends Storage {

  private final Chronicle chronicle;

  /**
   * @param maximumSize 
   * @param chronicle
   */
  public ChronicleStorage(final int maximumSize, final Chronicle chronicle) {
    super(maximumSize);

    this.chronicle = Preconditions.checkNotNull(chronicle, "null chronicle");
  }

  @Override
  public void append(final long timestamp, final int[] consolidates) throws IOException {
    final Excerpt excerpt = this.chronicle.createExcerpt();
    excerpt.startExcerpt(8+4+4*consolidates.length);
    excerpt.writeLong(timestamp);
    excerpt.writeInt(consolidates.length);
    for (final int consolidate : consolidates) {
      excerpt.writeInt(consolidate);
    }
    excerpt.finish();
  }

  @Override
  public Iterable<Pair<Long, int[]>> all() throws IOException {
    return new Iterable<Pair<Long, int[]>>() {
      @Override
      public Iterator<Pair<Long, int[]>> iterator() {
        return new AbstractIterator<Pair<Long, int[]>>() {
          private final Excerpt excerpt = ChronicleStorage.this.chronicle.createExcerpt();
          @Override
          protected Pair<Long, int[]> computeNext() {
            if (this.excerpt.nextIndex()) {
              final long timestamp = this.excerpt.readLong();
              final int nbConsolidates = this.excerpt.readInt();
              final int[] consolidates = new int[nbConsolidates];
              for (int i = 0; i < nbConsolidates; i++) {
                consolidates[i] = this.excerpt.readInt();
              }
              this.excerpt.finish();
              return new Pair<Long, int[]>(timestamp, consolidates);
            }
            return endOfData();
          }
        };
      }
    };
  }

}