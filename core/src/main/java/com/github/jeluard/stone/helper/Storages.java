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
package com.github.jeluard.stone.helper;

import com.github.jeluard.stone.api.ConsolidationListener;
import com.github.jeluard.stone.spi.Storage;
import com.google.common.base.Preconditions;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Helper methods for {@link Storage}.
 */
public final class Storages {

  private Storages() {
  }

  /**
   * @param storage
   * @param logger
   * @return {@code Storage} wrapped as a {@link ConsolidationListener}
   */
  public static ConsolidationListener asConsolidationListener(final Storage storage, final Logger logger) {
    Preconditions.checkNotNull(storage, "null storage");

    return new ConsolidationListener() {
      @Override
      public void onConsolidation(final long timestamp, final int[] consolidates) {
        try {
          storage.append(timestamp, consolidates);
        } catch (IOException e) {
          if (logger.isLoggable(Level.WARNING)) {
            logger.log(Level.WARNING, "Got exception while appending", e);
          }
        }
      }
    };
  }
  
}
