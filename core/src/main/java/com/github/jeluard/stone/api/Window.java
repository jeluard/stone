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
import java.util.Arrays;

import javax.annotation.concurrent.Immutable;

import org.joda.time.Duration;

/**
 * Encapsulate details about how published data will be kept.
 */
@Immutable
public final class Window {

  private final Duration resolution;
  private final Duration duration;
  private final ConsolidationListener[] consolidationListeners;

  public Window(final Duration resolution, final Duration duration, final ConsolidationListener ... consolidationListeners) {
    this.resolution = Preconditions.checkNotNull(resolution, "null resolution");
    this.duration = Preconditions.checkNotNull(duration, "null duration");
    this.consolidationListeners = Preconditions.checkNotNull(consolidationListeners, "null consolidationListeners");
  }

  public Duration getResolution() {
    return this.resolution;
  }

  public Duration getDuration() {
    return this.duration;
  }

  public ConsolidationListener[] getConsolidationListeners() {
    return this.consolidationListeners;
  }

  @Override
  public int hashCode() {
    return this.resolution.hashCode() + this.duration.hashCode() + Arrays.hashCode(this.consolidationListeners);
  }

  @Override
  public boolean equals(final Object object) {
    if (!(object instanceof Window)) {
      return false;
    }

    final Window other = (Window) object;
    return this.resolution.equals(other.resolution) && this.duration.equals(other.duration) && Arrays.equals(this.consolidationListeners, other.consolidationListeners);
  }

  @Override
  public String toString() {
    return "resolution <"+this.resolution+"> duration <"+this.duration+"> consolidationListeners <"+Arrays.asList(this.consolidationListeners)+">";
  }

}