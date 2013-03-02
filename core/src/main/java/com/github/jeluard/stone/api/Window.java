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

import com.github.jeluard.guayaba.base.Preconditions2;
import com.github.jeluard.stone.helper.Loggers;
import com.google.common.base.Preconditions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Level;

import javax.annotation.concurrent.Immutable;

/**
 * Encapsulate details about how published data will be kept.
 */
@Immutable
public final class Window {

  /**
   * Builder for {@link Window}.
   */
  public static final class Builder {

    private final int size;
    private final List<ConsolidationListener> consolidationListeners = new LinkedList<ConsolidationListener>();

    private Builder(final int size) {
      this.size = Preconditions2.checkSize(size);
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

    public Window consolidatedBy(final Class<? extends Consolidator> ... consolidatorTypes) {
      return new Window(this.size, Arrays.asList(consolidatorTypes), this.consolidationListeners);
    }

  }

  private final int size;
  private final List<? extends Class<? extends Consolidator>> consolidatorTypes;
  private final List<? extends ConsolidationListener> consolidationListeners;

  private Window(final int size, final List<? extends Class<? extends Consolidator>> consolidatorTypes, final List<? extends ConsolidationListener> consolidationListeners) {
    this.size = size;
    this.consolidatorTypes = Collections.unmodifiableList(new ArrayList<Class<? extends Consolidator>>(Preconditions2.checkNotEmpty(consolidatorTypes, "null consolidatorTypes")));
    this.consolidationListeners = Collections.unmodifiableList(new ArrayList<ConsolidationListener>(Preconditions.checkNotNull(consolidationListeners, "null consolidationListeners")));

    if (consolidationListeners.isEmpty()) {
      if (Loggers.BASE_LOGGER.isLoggable(Level.INFO)) {
        Loggers.BASE_LOGGER.log(Level.INFO, "No {0}: all data will be discarded", ConsolidationListener.class.getSimpleName());
      }
    }
  }

  public static Window.Builder of(final int size) {
    return new Window.Builder(size);
  }

  public int getSize() {
    return this.size;
  }

  public List<? extends Class<? extends Consolidator>> getConsolidatorTypes() {
    return this.consolidatorTypes;
  }

  public List<? extends ConsolidationListener> getConsolidationListeners() {
    return this.consolidationListeners;
  }

}