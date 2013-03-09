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
package com.github.jeluard.stone.example;

import com.github.jeluard.guayaba.util.concurrent.Scheduler;
import com.github.jeluard.stone.api.Window;
import com.github.jeluard.stone.dispatcher.sequential.SequentialDispatcher;
import com.github.jeluard.stone.helper.Loggers;
import com.github.jeluard.stone.pattern.Poller;
import com.github.jeluard.stone.storage.memory.MemoryStorageFactory;
import com.google.common.base.Function;
import com.google.common.util.concurrent.AbstractFuture;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Future;

public class PollerExample {

  static class DummyFuture extends AbstractFuture<Integer> {
    private final int result;
    public DummyFuture(final int result) {
      this.result = result;
    }
    public void done() {
      set(this.result);
    }
  }
  static class Triggerer implements Runnable {

    private final List<DummyFuture> futures = new CopyOnWriteArrayList<DummyFuture>();

    @Override
    public void run() {
      while (!Thread.currentThread().isInterrupted()) {
        try {
          Thread.sleep(1);
        } catch (InterruptedException ex) {
        }

        for (final Iterator<DummyFuture> it = this.futures.iterator(); it.hasNext();) {
          final DummyFuture abstractFuture = it.next();
          abstractFuture.done();
          this.futures.remove(abstractFuture);
        }
      }
    }
    private void enqueue(final DummyFuture abstractFuture) {
      this.futures.add(abstractFuture);
    }
  }

  public static String randomString(final int size) {
    final StringBuilder builder = new StringBuilder(size);
    for (int i = 0; i < size; i++) {
      builder.append(Math.random());
    }
    return builder.toString();
  }

  public static void main(final String[] main) throws Exception {
    final Triggerer triggerer = new Triggerer();
    final List<Window> windows = Arrays.asList();
    final Poller<String> poller = new Poller<String>(1000, windows, Poller.<String>defaultIdExtractor(), new Function<String, Future<Integer>>() {
      @Override
      public Future<Integer> apply(final String input) {
        final int length = input.length();
        final DummyFuture abstractFuture=  new DummyFuture(length);
        triggerer.enqueue(abstractFuture);
        return abstractFuture;
      }
    }, new SequentialDispatcher(), new MemoryStorageFactory(), Scheduler.defaultExecutorService(10, Loggers.BASE_LOGGER));

    final Thread thread = new Thread(triggerer, "Triggerer");
    thread.start();

    poller.enqueue("abcdefghi");
    poller.enqueue("abcdefghij");

    poller.start();

    Thread.sleep(1000);

    poller.enqueue("abcdefghijk");
    poller.dequeue("abcdefghi");

    Thread.sleep(1000);

    for (int i = 0; i < 10; i++) {
      poller.enqueue(randomString(10));
    }

    Thread.sleep(10000);

    poller.cancel();
  }

}