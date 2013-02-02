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

import com.github.jeluard.guayaba.annotation.Idempotent;
import com.github.jeluard.stone.api.Window;

import java.io.Closeable;
import java.io.IOException;
import javax.annotation.concurrent.ThreadSafe;

import org.joda.time.Duration;

/**
 * Abstract creation of {@link Storage} backend per persistent {@link Window}.
 * <br />
 * Common structure can then be shared accross {@link Storage}.
 */
@ThreadSafe
public interface StorageFactory<T extends Storage> extends Closeable {

  /**
   * Create or open a {@link Storage} specific to provided {@code id}, {@link Archive} and {@link Window}.
   * This {@link Storage} will then only be used to persist associated data.
   * Internal resources can be shared but {@link Storage} methods should be isolated to others {@code id}, {@link Archive} and {@link Window}.
   * <br>
   * At this stage {@link Storage} is initialized and ready to be used.
   * <br>
   * No caching should be done here as this is done by the caller.
   *
   * @param id unique id of associated {@link com.github.jeluard.stone.api.TimeSeries}
   * @param window associated {@link Window}
   * @param duration
   * @return a fully initialized {@link Storage}
   * @throws IOException 
   */
  T createOrGet(String id, Window window, Duration duration) throws IOException;

  /**
   * Close the {@link Storage} created for this triple.
   *
   * @param id
   * @param window
   * @throws IOException 
   */
  @Idempotent
  void close(String id, Window window) throws IOException;

  /**
   * {@inheritDoc}
   */
  @Idempotent
  @Override
  void close() throws IOException;

  /**
   * Delete all {@link Storage} generated data associated to timeseries {@code id}.
   *
   * @param id
   * @throws IOException 
   */
  @Idempotent
  void delete(String id) throws IOException;

}