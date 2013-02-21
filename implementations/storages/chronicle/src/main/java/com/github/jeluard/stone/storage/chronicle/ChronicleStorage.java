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
import com.github.jeluard.stone.api.Window;
import com.github.jeluard.stone.spi.Storage;
import com.google.common.base.Preconditions;
import com.google.common.collect.AbstractIterator;
import com.higherfrequencytrading.chronicle.Chronicle;
import com.higherfrequencytrading.chronicle.Excerpt;

import java.io.IOException;
import java.util.Iterator;

/**
 */
public final class ChronicleStorage extends Storage {

  private final Chronicle chronicle;

  /**
   * @param window 
   * @param journal life-cycle is not handled here, expect a fully {@link Journal#open()} {@code journal}
   */
  public ChronicleStorage(final Window window, final Chronicle chronicle) throws IOException {
    super(window);

    this.chronicle = Preconditions.checkNotNull(chronicle, "null chronicle");
  }

  @Override
  public void onConsolidation(final long timestamp, final int[] consolidates) throws IOException {
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
          final Excerpt excerpt = chronicle.createExcerpt();
          @Override
          protected Pair<Long, int[]> computeNext() {
            if (excerpt.nextIndex()) {
              final long timestamp = excerpt.readLong();
              final int nbConsolidates = excerpt.readInt();
              final int[] consolidates = new int[nbConsolidates];
              for (int i = 0; i < nbConsolidates; i++) {
                consolidates[i] = excerpt.readInt();
              }
              excerpt.finish();
              return new Pair<Long, int[]>(timestamp, consolidates);
            }
            return endOfData();
          }
        };
      }
    };
  }

}