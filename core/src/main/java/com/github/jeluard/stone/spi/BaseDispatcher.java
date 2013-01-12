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

import com.google.common.base.Preconditions;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 *
 */
public abstract class BaseDispatcher implements Dispatcher {

  private List<Consolidator> consolidators = new CopyOnWriteArrayList<Consolidator>();
  private List<Consolidator> readOnlyConsolidators = Collections.unmodifiableList(this.consolidators);

  @Override
  public final boolean addConsolidator(final Consolidator consolidator) {
    Preconditions.checkNotNull(consolidator, "null consolidator");

    return this.consolidators.add(consolidator);
  }

  @Override
  public final boolean removeConsolidator(final Consolidator consolidator) {
    Preconditions.checkNotNull(consolidator, "null consolidator");

    return this.consolidators.remove(consolidator);
  }

  protected final List<Consolidator> getConsolidators() {
    return this.readOnlyConsolidators;
  }

}