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
package com.github.jeluard.stone.api;

import com.google.common.base.Preconditions;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Encapsulate details about how published values will be persisted.
 */
public final class Archive {

  private final Collection<Class<? extends Consolidator>> consolidators;
  private final List<Window> windows;

  public Archive(final Collection<? extends Class<? extends Consolidator>> consolidators, final List<Window> windows) {
    this.consolidators = Collections.unmodifiableCollection(new ArrayList<Class<? extends Consolidator>>(Preconditions.checkNotNull(consolidators, "null consolidators")));
    this.windows = Collections.unmodifiableList(new ArrayList<Window>(Preconditions.checkNotNull(windows, "null windows")));
  }

  public Collection<Class<? extends Consolidator>> getConsolidators() {
    return this.consolidators;
  }

  public List<Window> getWindows() {
    return this.windows;
  }

}