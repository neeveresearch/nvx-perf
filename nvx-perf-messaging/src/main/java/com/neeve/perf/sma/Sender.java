/**
 * Copyright 2016 Neeve Research, LLC
 *
 * This product includes software developed at Neeve Research, LLC
 * (http://www.neeveresearch.com/) as well as software licenced to
 * Neeve Research, LLC under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional information
 * regarding copyright ownership.
 *
 * Neeve Research licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at:
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.neeve.perf.sma;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Set;

import com.lmax.disruptor.BatchEventProcessor;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.MultiThreadedLowContentionClaimStrategy;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.SequenceBarrier;

import com.neeve.ci.XRuntime;
import com.neeve.event.Event;
import com.neeve.event.IEventHandler;
import com.neeve.perf.serialization.CarFactory;
import com.neeve.sma.MessageChannel;
import com.neeve.sma.MessageView;
import com.neeve.sma.SmaException;
import com.neeve.tools.interactive.commands.AnnotatedCommand;
import com.neeve.util.UtlGovernor;
import com.neeve.util.UtlProps;
import com.neeve.util.UtlThread;
import com.neeve.util.UtlTime;

/**
 * Sender to benchmark SMA performance
 */
@AnnotatedCommand.Command(keywords = "Sender", description = "A sender to benchmark SMA Performance")
final public class Sender extends Common implements IEventHandler {
    final private class DetachedSender {
        final private class CarrierEvent {
            MessageChannel channel;
            MessageView message;

            CarrierEvent() {
            }

            final void reset() {
                channel = null;
                message.dispose();
                message = null;
            }
        };

        final private class CarrierEventProcessor implements EventHandler<CarrierEvent> {
            final public void onEvent(final CarrierEvent event,
                                      final long sequence,
                                      final boolean endOfBatch) throws Exception {
                try {
                    Sender.this.sendMessage(event.message, event.channel);
                }
                catch (Throwable e) {
                    e.printStackTrace();
                }
                finally {
                    event.reset();
                }
            }
        };

        final private class SenderThread extends Thread {
            final private long affinity;

            SenderThread(final String name, 
                         final BatchEventProcessor<CarrierEvent> batchProcessor,
                         final long affinity) {
                super(batchProcessor);
                this.affinity = affinity;
                setDaemon(true);
                setName(name);
            }

            @Override
            final public void run() {
                UtlThread.setCPUAffinityMask(affinity);
                super.run();
            }
        }

        final private int id;
        final private RingBuffer<CarrierEvent> ringBuffer;
        final private MessageChannel channel;
        final private SenderThread senderThread;

        DetachedSender(final int id, final MessageChannel channel) {
            // store id
            this.id = id;

            // create the disruptor
            ringBuffer = new RingBuffer<CarrierEvent>(new EventFactory<CarrierEvent>() {
                @Override
                final public CarrierEvent newInstance() {
                    return new CarrierEvent();
                }
            }, new MultiThreadedLowContentionClaimStrategy(64), XRuntime.createWaitStrategy("Blocking", true));
            final BatchEventProcessor batchProcessor = new BatchEventProcessor<CarrierEvent>(ringBuffer, 
                                                                                             ringBuffer.newBarrier(), 
                                                                                             new CarrierEventProcessor());
            ringBuffer.setGatingSequences(batchProcessor.getSequence());
            (senderThread = new SenderThread("sender-" + id, 
                                             batchProcessor,
                                             UtlThread.parseAffinityMask(XRuntime.getValue("affinity." + id, "0")))).start();
            try {
                Thread.sleep(100);
            }
            catch (InterruptedException e) {}

            // store the channel
            this.channel = channel;
        }

        final void sendMessage() {
            final MessageView message = carFactory.createCar(populate);
            final long sequence = ringBuffer.next();
            final CarrierEvent carrierEvent = ringBuffer.get(sequence);
            carrierEvent.channel = channel;
            carrierEvent.message = message;
            ringBuffer.publish(sequence);
        }
    }

    /*
     * Configuration options
     */
    @Option(shortForm = 't', longForm = "senders", required = false, defaultValue = "1", description = "The number of sender threads to use")
    private int numSenders;
    @Option(shortForm = 'r', longForm = "rate", required = true, defaultValue = "-1", description = "The send rate. If less than 1 then unlimited")
    private int rate;
    @Option(shortForm = 'i', longForm = "size", required = false, defaultValue = "256", description = "the message data size.")
    private int size;
    @Option(shortForm = 'p', longForm = "populate", defaultValue = "true", description = "populate outbound messages with full content (otherwise only timestamp is sent)")
    boolean populate;

    /*
     * Private scope members
     */
    final private CarFactory carFactory;
    final private List<DetachedSender> detachedSenders;
    final private Random random;
    private boolean done;

    public Sender() {
        carFactory = new CarFactory(encoding);
        detachedSenders = new ArrayList<DetachedSender>();
        random = new Random(System.currentTimeMillis());
    }

    final private void sendMessage(final MessageView message, final MessageChannel channel) throws SmaException {
        channel.sendMessage(message, null, MessageChannel.ALREADY_SYNCD | MessageChannel.KEY_ALREADY_RESOLVED | MessageChannel.KEY_ALREADY_VALIDATED );
    }

    @Override
    final public void onEvent(final Event event) {}

    final public void interrupt(Thread commandThread) {
        done = true;
    }

    @Override
    final protected void doRun() throws Exception {
        // initialize
        final boolean detachedSend = numSenders > 1;
        done = false;

        // dump config
        System.out.println("Streaming Sender");
        System.out.println("  Bus.............." + busDescriptorString);
        System.out.println("  Send Count......." + (count >= 1 ? count : "Unlimited"));
        System.out.println("  Send Rate........" + (rate >= 1 ? "" + rate : "Unlimited"));
        System.out.println("  Send Size........" + size);
        System.out.println("  Populate........." + populate);
        System.out.println("  Num Senders......" + numSenders);
        System.out.println("  Encoding........." + encoding);
        System.out.println("  Key.............." + channelKey);
        System.out.println("  Qos.............." + qos);
        System.out.println("  nv.optimizefor..." + (XRuntime.optimizeForThroughput() ? "Throughput" : (XRuntime.optimizeForLatency() ? "Latency" : "None")));

        // connect
        connect(false);

        // set affinity
        UtlThread.parseAffinityMask(XRuntime.getValue("affinity.0", "0"));

        // create detached senders
        if (detachedSend) {
            for (int i = 0 ; i < numSenders ; i++) {
                detachedSenders.add(new DetachedSender(i+1, channel));
            }
        }

        // send
        System.out.println("Sending...");
        int i = 0;
        final long start = System.currentTimeMillis();
        final long statInterval = 1000;
        long istart = System.currentTimeMillis();
        int di = 0;
        UtlGovernor throttler = new UtlGovernor(rate);
        count = count < 1 ? Integer.MAX_VALUE : count;
        while (i < count && !done) {
            throttler.blockToNext();
            final long current = System.currentTimeMillis();
            if (detachedSend) {
                detachedSenders.get(random.nextInt(detachedSenders.size())).sendMessage();
            }
            else {
                final MessageView message = carFactory.createCar(populate);
                try {
                    sendMessage(message, channel);
                }
                finally { 
                    message.dispose();
                }
            }
            di++;
            i++;
            if (current - istart > 1000) { // every 1 second
                int deltaRate = (int)((di * statInterval) / (current - istart));
                int overallRate = (int)((i * statInterval) / (current - start));
                System.out.println("Sent=" + i + " DRate=" + deltaRate + " Rate=" + overallRate);
                istart = current;
                di = 0;
            }
        }
        try {
            binding.flush(null);
            binding.close();
        }
        catch (SmaException e) {
            throw new RuntimeException(e);
        }
        final long current = System.currentTimeMillis();
        final int overallRate = (int)((i * statInterval) / (current - start));
        System.out.println("Done (Sent=" + i + ", Rate=" + overallRate + ")");
        try {
            binding.close();
        }
        catch (SmaException e) {
            throw new RuntimeException(e);
        }
    }

    public static void main(String[] args) throws Exception {
        new Sender().run(args);
    }
}
