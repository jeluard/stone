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
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.joda.time.Duration;

/**
 *
 */
public final class TimeSerie {

  public static final class SamplingFrame {

    private final Duration resolution;
    private final Duration duration;

    public SamplingFrame(final Duration resolution, final Duration duration) {
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

  private final String id;
  private final List<Consolidator> consolidators;
  private List<TimeSerie.SamplingFrame> samplingFrames;

  public TimeSerie(final String id, final List<? extends Consolidator> consolidators, final TimeSerie.SamplingFrame samplingFrame) {
    this.id = Preconditions.checkNotNull(id, "null id");
    this.consolidators = new ArrayList<Consolidator>(Preconditions.checkNotNull(consolidators, "null consolidators"));
    this.samplingFrames = new CopyOnWriteArrayList<TimeSerie.SamplingFrame>(Arrays.asList(samplingFrame));
  }

  public TimeSerie then(final TimeSerie.SamplingFrame samplingFrame) {
    this.samplingFrames.add(samplingFrame);
    return this;
  }

  public String getId() {
    return this.id;
  }

  public List<Consolidator> getConsolidators() {
    return new ArrayList<Consolidator>(this.consolidators);
  }

  public List<SamplingFrame> getSamplingFrames() {
    return new ArrayList<SamplingFrame>(this.samplingFrames);
  }

}