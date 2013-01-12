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
package com.github.jeluard.stone.impl;

import com.github.jeluard.guayaba.lang.Cancelable;
import com.github.jeluard.stone.api.DataPoint;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.SingleThreadedClaimStrategy;
import com.lmax.disruptor.SleepingWaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import org.joda.time.Duration;
import org.joda.time.Period;

/**
 *
 * @author julien
 */
public class DisruptorDispatcher implements Cancelable {

  /**
   * {@link RingBuffer} event encapsulating a {@link DataPoint}.
   */
  private static final class DataPointEvent {

    private DataPoint dataPoint;

    /**
     * Unique {@link EventFactory} instance for {@link DataPoint}.
     */
    private static final EventFactory<DataPointEvent> EVENT_FACTORY = new EventFactory<DataPointEvent>() {
      @Override
      public DataPointEvent newInstance() {
        return new DataPointEvent();
      }
    };

  }

  private final Disruptor<DataPointEvent> disruptor;
  private final RingBuffer<DataPointEvent> ringBuffer;

  public DisruptorDispatcher(final Duration duration, final Period period) {

    final Executor executor = Executors.newFixedThreadPool(2);
    this.disruptor = new Disruptor<DataPointEvent>(DataPointEvent.EVENT_FACTORY, executor, 
                            new SingleThreadedClaimStrategy(32768), new SleepingWaitStrategy());

    /*final List<EventHandler> eventHandlers = Lists.transform(Arrays.asList(sinks), new Function<Sink, EventHandler>() {
      @Override
      public EventHandler apply(final Sink input) {
        Preconditions.checkNotNull(input, "null input");

        return new EventHandler<DataPointEvent>() {
          @Override
          public void onEvent(final DataPointEvent event, final long sequence, final boolean endOfBatch) throws Exception {
            Preconditions.checkNotNull(event, "null event");

            final DataPoint dataPoint = event.dataPoint;
            publish(dataPoint);
          }

        };
      }
    });
    this.disruptor.handleEventsWith(eventHandlers.toArray(new EventHandler[eventHandlers.size()]));*/
    this.ringBuffer = this.disruptor.start();
  }

  public void publish(DataPoint dataPoint) {

    final long sequence = this.ringBuffer.next();
    final DataPointEvent event = this.ringBuffer.get(sequence);
  //  event.dataPoint = value;
    this.ringBuffer.publish(sequence);
  }

  @Override
  public void cancel() {
    //TODO force append stop
    this.disruptor.shutdown();
  }

}