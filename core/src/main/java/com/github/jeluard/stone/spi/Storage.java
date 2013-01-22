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

import com.github.jeluard.stone.api.Reader;

import java.io.IOException;

/**
 * Abstraction dealing with {@link TimeSeries} persistency.
 * <br>
 * A {@link Storage} is specific to a single {@link Window} of a {@link TimeSeries}.
 */
public interface Storage extends Reader {

  /**
   * Append a {@code timestamp}/{@code consolidates} pair.
   * <br>
   * At this point {@code consolidates} has been validated and is not null.
   *
   * @param timestamp the beginning of the associated window
   * @param consolidates
   * @throws IOException 
   */
  void append(long timestamp, int[] consolidates) throws IOException;

}