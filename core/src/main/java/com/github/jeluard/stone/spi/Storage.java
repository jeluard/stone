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

import com.github.jeluard.stone.api.DataAggregates;
import com.github.jeluard.stone.api.DataPoint;
import com.google.common.base.Optional;

import java.io.IOException;
import org.joda.time.DateTime;

import org.joda.time.Interval;

/**
 * Abstraction dealing with {@link TimeSeries} persistency.
 */
public interface Storage {

  void append(DataAggregates aggregates) throws IOException;

  Optional<DateTime> last() throws IOException;

  Iterable<DataAggregates> all() throws IOException;

  Iterable<DataAggregates> during(Interval interval) throws IOException;

}