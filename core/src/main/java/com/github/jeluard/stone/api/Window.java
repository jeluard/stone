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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import javax.annotation.concurrent.Immutable;

import org.joda.time.Duration;

/**
 * Encapsulate details about how published data will be kept.
 */
@Immutable
public final class Window {

  /**
   * Builder for {@link Window}.
   */
  public static final class Builder {

    private final Duration resolution;
    private Duration persistedDuration;
    private final List<ConsolidationListener> consolidationListeners = new LinkedList<ConsolidationListener>();

    private Builder(final Duration resolution) {
      this.resolution = Preconditions.checkNotNull(resolution, "null resolution");
    }

    /**
     * Add {@link ConsolidationListener}s. Each call accumulates new values.
     *
     * @param consolidationListeners
     * @return this
     */
    public Window.Builder listenedBy(final ConsolidationListener ... consolidationListeners) {
      this.consolidationListeners.addAll(Arrays.asList(consolidationListeners));
      return this;
    }

    /**
     * Set the persisted {@link Duration}. If not set {@link Window} won't be persisted.
     * <br>
     * Last call win (i.e. override previous value).
     *
     * @param persistedDuration
     * @return this
     */
    public Window.Builder persistedDuring(final Duration persistedDuration) {
      this.persistedDuration = Preconditions.checkNotNull(persistedDuration, "null persistedDuration");
      return this;
    }

    public Window consolidatedBy(final Class<? extends Consolidator> ... consolidatorTypes) {
      return new Window(this.resolution, Optional.fromNullable(this.persistedDuration), Arrays.asList(consolidatorTypes), this.consolidationListeners);
    }

  }

  private final Duration resolution;
  private final Optional<Duration> optionalPersistedDuration;
  private final List<? extends Class<? extends Consolidator>> consolidatorTypes;
  private final List<? extends ConsolidationListener> consolidationListeners;

  public Window(final Duration resolution, final List<? extends Class<? extends Consolidator>> consolidatorTypes, final List<? extends ConsolidationListener> consolidationListeners) {
    this(resolution, Optional.<Duration>absent(), consolidatorTypes, consolidationListeners);
  }

  public Window(final Duration resolution, final Duration persistedDuration, final List<? extends Class<? extends Consolidator>> consolidatorTypes, final List<? extends ConsolidationListener> consolidationListeners) {
    this(resolution, Optional.of(Preconditions.checkNotNull(persistedDuration, "null persistedDuration")), consolidatorTypes, consolidationListeners);
  }

  private Window(final Duration resolution, final Optional<Duration> optionalPersistedDuration, final List<? extends Class<? extends Consolidator>> consolidatorTypes, final List<? extends ConsolidationListener> consolidationListeners) {
    this.resolution = Preconditions.checkNotNull(resolution, "null resolution");
    this.optionalPersistedDuration = Preconditions.checkNotNull(optionalPersistedDuration, "null optionalPersistedDuration");
    this.consolidatorTypes = Collections.unmodifiableList(new ArrayList<Class<? extends Consolidator>>(Preconditions.checkNotNull(consolidatorTypes, "null consolidatorTypes")));
    this.consolidationListeners = Collections.unmodifiableList(new ArrayList<ConsolidationListener>(Preconditions.checkNotNull(consolidationListeners, "null consolidationListeners")));
  }

  public static Window.Builder of(final Duration duration) {
    return new Window.Builder(duration);
  }

  public Duration getResolution() {
    return this.resolution;
  }

  public Optional<Duration> getPersistedDuration() {
    return this.optionalPersistedDuration;
  }

  public List<? extends Class<? extends Consolidator>> getConsolidatorTypes() {
    return this.consolidatorTypes;
  }

  public List<? extends ConsolidationListener> getConsolidationListeners() {
    return this.consolidationListeners;
  }

  @Override
  public int hashCode() {
    return this.resolution.hashCode() + this.consolidatorTypes.hashCode() + this.consolidationListeners.hashCode();
  }

  @Override
  public boolean equals(final Object object) {
    if (!(object instanceof Window)) {
      return false;
    }

    final Window other = (Window) object;
    return this.resolution.equals(other.resolution) && this.consolidatorTypes.equals(other.consolidatorTypes) && this.consolidationListeners.equals(other.consolidationListeners);
  }

  @Override
  public String toString() {
    return "resolution <"+this.resolution+"> consolidatorTypes <"+this.consolidatorTypes+"> consolidationListeners <"+this.consolidationListeners+">";
  }

}