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
import com.carrotsearch.junitbenchmarks.BenchmarkOptions;

import java.io.IOException;

import org.junit.Test;

@BenchmarkOptions(callgc = false, benchmarkRounds = 5, warmupRounds = 3)
public abstract class BaseStorageBenchmark<T extends Storage> extends AbstractBenchmark {

  protected abstract T createStorage(int maximumSize) throws IOException;

  @Test
  public void benchCreation() throws Exception {
    createStorage(10);
  }  

  @Test
  public void bench100Consolidation() throws Exception {
    final T storage = createStorage(10);
    final long now = System.currentTimeMillis();
    for (int i = 0; i < 100; i++) {
      storage.append(now+i, new int[]{i, i+1});
    }
  }

  @Test
  public void bench100ConsolidationNoRoll() throws Exception {
    final T storage = createStorage(101);
    final long now = System.currentTimeMillis();
    for (int i = 0; i < 100; i++) {
      storage.append(now+i, new int[]{i, i+1});
    }
  }

  @Test
  public void bench1KConsolidationNoRoll() throws Exception {
    final T storage = createStorage(1001);
    final long now = System.currentTimeMillis();
    for (int i = 0; i < 1000; i++) {
      storage.append(now+i, new int[]{i, i+1});
    }
  }

}