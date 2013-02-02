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

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;

import java.util.Arrays;
import java.util.List;

import javax.annotation.concurrent.Immutable;

import org.joda.time.Duration;

/**
 * Encapsulate details about how published data will be kept.
 */
@Immutable
public final class Window {

  private final Duration resolution;
  private final Optional<Duration> optionalPersistentDuration;
  private final Class<? extends Consolidator>[] consolidatorTypes;
  private final ConsolidationListener[] consolidationListeners;

  public Window(final Duration resolution, final List<? extends Class<? extends Consolidator>> consolidatorTypes, final ConsolidationListener ... consolidationListeners) {
    this(resolution, Optional.<Duration>absent(), consolidatorTypes, consolidationListeners);
  }

  public Window(final Duration resolution, final Duration persistentDuration, final List<? extends Class<? extends Consolidator>> consolidatorTypes, final ConsolidationListener ... consolidationListeners) {
    this(resolution, Optional.of(Preconditions.checkNotNull(persistentDuration, "null persistentDuration")), consolidatorTypes, consolidationListeners);
  }

  private Window(final Duration resolution, final Optional<Duration> optionalPersistentDuration, final List<? extends Class<? extends Consolidator>> consolidatorTypes, final ConsolidationListener ... consolidationListeners) {
    this.resolution = Preconditions.checkNotNull(resolution, "null resolution");
    this.optionalPersistentDuration = Preconditions.checkNotNull(optionalPersistentDuration, "null optionalPersistentDuration");
    this.consolidatorTypes = Preconditions.checkNotNull(consolidatorTypes, "null consolidatorTypes").toArray(new Class[consolidatorTypes.size()]);
    this.consolidationListeners = Preconditions.checkNotNull(consolidationListeners, "null consolidationListeners");
  }

  public Duration getResolution() {
    return this.resolution;
  }

  public Optional<Duration> getPersistentDuration() {
    return this.optionalPersistentDuration;
  }

  public Class<? extends Consolidator>[] getConsolidatorTypes() {
    return this.consolidatorTypes;
  }

  public ConsolidationListener[] getConsolidationListeners() {
    return this.consolidationListeners;
  }

  @Override
  public int hashCode() {
    return this.resolution.hashCode() + Arrays.hashCode(this.consolidatorTypes) + Arrays.hashCode(this.consolidationListeners);
  }

  @Override
  public boolean equals(final Object object) {
    if (!(object instanceof Window)) {
      return false;
    }

    final Window other = (Window) object;
    return this.resolution.equals(other.resolution) && Arrays.equals(this.consolidatorTypes, other.consolidatorTypes) && Arrays.equals(this.consolidationListeners, other.consolidationListeners);
  }

  @Override
  public String toString() {
    return "resolution <"+this.resolution+"> consolidatorTypes <"+this.consolidatorTypes+"> consolidationListeners <"+Arrays.asList(this.consolidationListeners)+">";
  }

}