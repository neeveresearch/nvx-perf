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

import java.text.NumberFormat;

import com.neeve.ci.XRuntime;
import com.neeve.event.Event;
import com.neeve.perf.serialization.CarFactory;
import com.neeve.sma.MessageLatencyManager;
import com.neeve.sma.MessageView;
import com.neeve.sma.event.MessageBusBindingFailedEvent;
import com.neeve.sma.event.MessageEvent;
import com.neeve.sma.event.NonXMessageEvent;
import com.neeve.sma.event.UnhandledMessageEvent;
import com.neeve.stats.StatsFactory;
import com.neeve.stats.IStats.Latencies;
import com.neeve.tools.interactive.commands.AnnotatedCommand;
import com.neeve.util.UtlTime;

/**
 * Receiver to benchmark SMA performance
 */
@AnnotatedCommand.Command(keywords = "Receiver", description = "A receiver to benchmark SMA Performance")
final public class Receiver extends Common {
    final private class Stats {
        final NumberFormat format;
        final StringBuilder sb = new StringBuilder(2048);
        Latencies latencies = null;
        long startTime = -1;
        long postWarmUpStartTime = 0;
        long endTime = -1;
        long numTotalMsgs = 0;
        long numTotalOOO = 0;
        long postWarmUpCount = 0;
        long deltaStartTime;
        long numDeltaMsgs = 0;
        long numDeltaOOO = 0;
        long lastSno = -1;
        boolean warmUpCompleted = false;

        Stats() {
            format = NumberFormat.getInstance();
            format.setMaximumFractionDigits(2);
        }
    }

    /*
     * Private scope members
     */
    final private CarFactory carFactory;
    final private Stats stats;
    private MessageLatencyManager latencyManager;
    private boolean done = false;

    /*
     * Constructor
     */
    public Receiver() {
        carFactory = new CarFactory(encoding);
        stats = new Stats();
    }

    @Override
    final protected void doRun() throws Exception {
        done = false;
        System.out.println("SMA Streaming Receiver");
        System.out.println("  Bus....................." + busDescriptorString);
        System.out.println("  Encoding................" + encoding);
        System.out.println("  Receive Count..........." + count);
        System.out.println("  Key....................." + channelKey);
        System.out.println("  Filter.................." + channelFilter);
        System.out.println("  Qos....................." + qos);
        System.out.println("  nv.optimizefor.........." + (XRuntime.optimizeForThroughput() ? "Throughput" : (XRuntime.optimizeForLatency() ? "Latency" : "None")));
        System.out.println("  nv.optimizeMemoryUsage.." + XRuntime.optimizeMemoryUsage());
        connect(true);
        latencyManager = binding.getLatencyManager();
        stats.latencies = StatsFactory.createLatencyStat("Received", Math.max(1000000, count));
        try {
            while (!done) {
                Thread.sleep(100);
            }
            channel.leave(0);
            binding.close();
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void interrupt(Thread commandThread) {
        done = true;
    }

    final private void processMessage(final long sequenceNumber, final long sendTimestamp, final long receiveTimestamp) {
        long currentTime = System.currentTimeMillis();
        if (stats.startTime == -1) {
            stats.startTime = stats.deltaStartTime = currentTime;
            stats.numTotalMsgs = stats.numDeltaMsgs = 1;
            stats.lastSno = sequenceNumber;
        }
        else {
            if (stats.warmUpCompleted) {
                stats.postWarmUpCount++;
            }
            else if (currentTime - stats.startTime > 15000) { // 15 second warmup
                System.out.println("Warm up complete.");
                stats.postWarmUpStartTime = currentTime;
                stats.warmUpCompleted = true;
                stats.latencies.reset();
            }

            if (stats.latencies != null && sendTimestamp > 0) {
                int latency = (int)(receiveTimestamp - sendTimestamp);
                stats.latencies.add(latency);
            }

            final double deltaTime = currentTime - stats.deltaStartTime;
            final double elapsedTime = currentTime - stats.startTime;
            final boolean ooo = sequenceNumber != (stats.lastSno+1);
            stats.numTotalMsgs++;
            if (ooo) stats.numTotalOOO++;
            stats.numDeltaMsgs++;
            if (ooo) stats.numDeltaOOO++;
            stats.lastSno = sequenceNumber;
            final long numTotalMsgsLost = stats.lastSno - stats.numTotalMsgs;
            if (currentTime - stats.deltaStartTime > 1000) {
                final int totalLossPct = numTotalMsgsLost > 0 ? (int)((((double)numTotalMsgsLost) / stats.numTotalMsgs) * 100) : 0;
                final int totalRate = stats.numTotalMsgs > 0 ? (int)(((stats.numTotalMsgs) / elapsedTime) * 1000) : 0;
                final int deltaRate = stats.numDeltaMsgs > 0 ? (int)(((stats.numDeltaMsgs) / deltaTime) * 1000) : 0;
                stats.sb.setLength(0);
                stats.sb.append("[COUNTS (").append(currentTime).append(")]");
                stats.sb.append(" NumRcvd=").append(stats.numTotalMsgs).append("(" + stats.numDeltaMsgs + ")");
                stats.sb.append(" LastSno=").append(stats.lastSno);
                stats.sb.append(" Rate=").append(totalRate).append("(" + deltaRate + ")");
                stats.sb.append(" Loss=").append(numTotalMsgsLost).append("[").append(totalLossPct).append("%]");
                stats.sb.append(" OOO=").append(stats.numTotalOOO).append("(").append(stats.numDeltaOOO).append(")\n");
                stats.sb.append("[LATENCIES (").append(currentTime).append(")]");
                stats.latencies.compute();
                stats.latencies.get(stats.sb, stats.format);
                stats.sb.append("\n");
                System.out.println(stats.sb.toString());
                stats.deltaStartTime = currentTime;
                stats.numDeltaMsgs = 0;
            }
            if (stats.numTotalMsgs == count) {
                done = true;
                stats.endTime = System.currentTimeMillis();
                final double postWarmupElapsedTime = stats.endTime - stats.postWarmUpStartTime;
                final int totalLossPct = numTotalMsgsLost > 0 ? (int)((((double)numTotalMsgsLost) / stats.numTotalMsgs) * 100) : 0;
                final int totalRate = stats.postWarmUpCount > 0 ? (int)(((stats.postWarmUpCount) / postWarmupElapsedTime) * 1000) : 0;
                stats.sb.setLength(0);
                stats.sb.append("[COUNTS (").append(stats.endTime).append(")]");
                stats.sb.append(" NumRcvd=").append(stats.numTotalMsgs);
                stats.sb.append(" LastSno=").append(stats.lastSno);
                stats.sb.append(" Rate=").append(totalRate);
                stats.sb.append(" Loss=").append(numTotalMsgsLost).append("[").append(totalLossPct).append("%]");
                stats.sb.append(" OOO=").append(stats.numTotalOOO).append("(").append(stats.numDeltaOOO).append(")\n");
                stats.sb.append("[LATENCIES (").append(stats.endTime).append(")]");
                stats.latencies.compute();
                stats.latencies.get(stats.sb, stats.format);
                System.out.println(stats.sb.toString());
            }
        }
        // allow auto-ack to take care of it.
    }

    @Override
    final public void onEvent(final Event event) {
        if (event instanceof MessageEvent) {
            final MessageView message = ((MessageEvent)event).getMessageView();
            processMessage(message.getMessageSequenceNumber(),
                           carFactory.disposeCar(message),
                           UtlTime.nowSinceEpoch());
            if (MessageLatencyManager.captureMsgLatencyStats && latencyManager != null) latencyManager.update(message, MessageLatencyManager.MessagingDirection.Inbound);
        }
        else if (event instanceof MessageBusBindingFailedEvent) {
            System.out.println("Binding failure [" + ((MessageBusBindingFailedEvent)event).getCause() + "]");
            System.exit(0);
        }
        else if (event instanceof NonXMessageEvent) {
            System.out.println("Received non X message event [" + event + "]");
        }
        else if (event instanceof UnhandledMessageEvent) {
            System.out.println("Received unhandled message event [" + event + "]");
        }
        else {
            System.out.println("Received unprocessed event [" + event + "]");
        }
    }

    public static void main(String[] args) throws Exception {
        new Receiver().run(args);
    }
}
