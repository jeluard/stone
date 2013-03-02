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

import com.github.jeluard.guayaba.base.Preconditions2;
import com.github.jeluard.guayaba.lang.Iterables2;
import com.github.jeluard.stone.api.Consolidator;
import com.github.jeluard.stone.api.Window;
import com.google.common.base.Preconditions;

import java.lang.reflect.Constructor;
import java.util.List;
import java.util.logging.Level;

/**
 * Helper methods for {@link Consolidator}.
 */
public final class Consolidators {

  private Consolidators() {
  }

  /**
   * Instantiate a {@link Consolidator} from specified {@code type}.
   * First look for a constructor accepting an int as unique argument then fallback to the default constructor.
   *
   * @param type
   * @param maxSamples
   * @return a {@link Consolidator} from specified {@code type}
   */
  public static Consolidator createConsolidator(final Class<? extends Consolidator> type, final int maxSamples) {
    Preconditions.checkNotNull(type, "null type");
    Preconditions2.checkSize(maxSamples);

    try {
      //First look for a constructor accepting int as argument
      try {
        return type.getConstructor(int.class).newInstance(maxSamples);
      } catch (NoSuchMethodException e) {
        if (Loggers.BASE_LOGGER.isLoggable(Level.FINEST)) {
          Loggers.BASE_LOGGER.log(Level.FINEST, "{0} does not define a constructor accepting int", type.getCanonicalName());
        }
      }

      //If we can't find such fallback to defaut constructor
      try {
        final Constructor<? extends Consolidator> defaultConstructor = type.getConstructor();
        return defaultConstructor.newInstance();
      } catch (NoSuchMethodException e) {
        throw new IllegalArgumentException("Failed to find int or default constructor for "+type.getCanonicalName(), e);
      }
    } catch (ReflectiveOperationException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * @param consolidatorTypes
   * @param maxSamples
   * @return all {@link Consolidator} mapping to {@link Window#getConsolidatorTypes()}
   * @see #createConsolidator(java.lang.Class)
   */
  public static Consolidator[] createConsolidators(final List<? extends Class<? extends Consolidator>> consolidatorTypes, final int maxSamples) {
    Preconditions.checkNotNull(consolidatorTypes, "null consolidatorTypes");
    Preconditions2.checkSize(maxSamples);

    final Consolidator[] consolidators = new Consolidator[consolidatorTypes.size()];
    for (final Iterables2.Indexed<? extends Class<? extends Consolidator>> indexed : Iterables2.withIndex(consolidatorTypes)) {
      consolidators[indexed.index] = createConsolidator(indexed.value, maxSamples);
    }
    return consolidators;
  }

}