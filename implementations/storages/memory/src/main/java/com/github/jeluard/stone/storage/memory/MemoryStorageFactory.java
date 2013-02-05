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
package com.github.jeluard.stone.storage.memory;

import com.github.jeluard.stone.api.Window;
import com.github.jeluard.stone.spi.StorageFactory;

import java.io.IOException;

import org.joda.time.Duration;

/**
 * {@link StorageFactory} creating memory based {@link com.github.jeluard.stone.spi.Storage}.
 */
public class MemoryStorageFactory extends StorageFactory<MemoryStorage> {

  @Override
  protected MemoryStorage create(final String id, final Window window, final Duration duration) throws IOException {
    return new MemoryStorage((int) (duration.getMillis() / window.getResolution().getMillis()), window.getConsolidatorTypes().size());
  }

}