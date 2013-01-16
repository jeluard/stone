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

import com.github.jeluard.stone.spi.Consolidator;
import com.google.common.base.Preconditions;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Encapsulate details about how published values will be persisted.
 */
public final class Archive {

  private final Collection<? extends Consolidator> consolidators;
  private final List<SamplingWindow> samplingWindows;

  public Archive(final Collection<? extends Consolidator> consolidators, final List<SamplingWindow> samplingWindows) {
    this.consolidators = new ArrayList<Consolidator>(Preconditions.checkNotNull(consolidators, "null consolidators"));
    this.samplingWindows = new ArrayList<SamplingWindow>(Preconditions.checkNotNull(samplingWindows, "null samplingWindows"));
  }

  public Collection<Consolidator> getConsolidators() {
    return Collections.unmodifiableCollection(this.consolidators);
  }

  public List<SamplingWindow> getSamplingWindows() {
    return this.samplingWindows;
  }

}