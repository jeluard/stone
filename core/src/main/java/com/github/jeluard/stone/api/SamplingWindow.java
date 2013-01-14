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
package com.github.jeluard.stone.api;

import com.google.common.base.Preconditions;

import org.joda.time.Duration;

public final class SamplingWindow {

  private final Duration resolution;
  private final Duration duration;

  public SamplingWindow(final Duration resolution, final Duration duration) {
    this.resolution = Preconditions.checkNotNull(resolution, "null resolution");
    this.duration = Preconditions.checkNotNull(duration, "null duration");
  }

  public Duration getResolution() {
    return resolution;
  }

  public long getDuration() {
    return this.duration.getMillis();
  }

}