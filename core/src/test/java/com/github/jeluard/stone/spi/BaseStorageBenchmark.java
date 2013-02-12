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

import com.carrotsearch.junitbenchmarks.AbstractBenchmark;
import com.carrotsearch.junitbenchmarks.annotation.BenchmarkHistoryChart;
import com.carrotsearch.junitbenchmarks.annotation.BenchmarkMethodChart;
import com.carrotsearch.junitbenchmarks.annotation.LabelType;
import com.github.jeluard.stone.api.Window;
import com.github.jeluard.stone.consolidator.MaxConsolidator;
import com.github.jeluard.stone.consolidator.MinConsolidator;

import java.io.IOException;

import org.joda.time.Duration;
import org.junit.Test;

//@BenchmarkMethodChart(filePrefix = "benchmark-lists")
public abstract class BaseStorageBenchmark<T extends Storage> extends AbstractBenchmark {

  protected abstract T createStorage(Window window) throws IOException;

  private Window createWindow(final int maxSize) {
    return Window.of(Duration.standardSeconds(1)).persistedDuring(Duration.standardSeconds(maxSize)).consolidatedBy(MaxConsolidator.class, MinConsolidator.class);
  }

  @Test
  //@BenchmarkHistoryChart(labelWith = LabelType.CUSTOM_KEY, maxRuns = 20)
  public void bench100Consolidation() throws Exception {
    final T storage = createStorage(createWindow(10));
    final long now = System.currentTimeMillis();
    for (int i = 0; i < 100; i++) {
      storage.onConsolidation(now+Duration.standardSeconds(i).getMillis(), new int[]{i, i+1});
    }
  }

  @Test
  @BenchmarkHistoryChart(labelWith = LabelType.CUSTOM_KEY, maxRuns = 20)
  public void bench100ConsolidationNoRoll() throws Exception {
    final T storage = createStorage(createWindow(101));
    final long now = System.currentTimeMillis();
    for (int i = 0; i < 100; i++) {
      storage.onConsolidation(now+Duration.standardSeconds(i).getMillis(), new int[]{i, i+1});
    }
  }

  @Test
  @BenchmarkHistoryChart(labelWith = LabelType.CUSTOM_KEY, maxRuns = 20)
  public void bench1KConsolidationNoRoll() throws Exception {
    final T storage = createStorage(createWindow(1001));
    for (int i = 0; i < 1000; i++) {
      storage.onConsolidation(i, new int[]{i, i+1});
    }
  }

  /*@Test
  @BenchmarkHistoryChart(labelWith = LabelType.CUSTOM_KEY, maxRuns = 20)
  public void bench1MConsolidation() throws Exception {
    final T storage = createStorage(createWindow(1000));
    for (int i = 0; i < 1000*1000; i++) {
      storage.onConsolidation(i, new int[]{i, i+1});
    }
  }*/

}