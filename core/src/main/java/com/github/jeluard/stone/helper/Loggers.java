/*
 * Copyright 2013 julien.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.jeluard.stone.helper;

import com.google.common.base.Preconditions;

import java.util.logging.Logger;

/**
 * Helper methods for {@link Logger}.
 */
public class Loggers {

  private Loggers() {
  }

  private static final String BASE_LOGGER_NAME = "com.github.jeluard.stone";
  public static final Logger BASE_LOGGER = Logger.getLogger(Loggers.BASE_LOGGER_NAME);

  /**
   * @param suffix
   * @return a configured {@link Logger}
   */
  public static Logger create(final String suffix) {
    Preconditions.checkNotNull(suffix, "null suffix");

    return Logger.getLogger(Loggers.BASE_LOGGER_NAME+"."+suffix);
  }

}