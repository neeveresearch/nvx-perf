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
package com.neeve.perf.aep.mux;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Properties;

import com.neeve.ci.XRuntime;
import com.neeve.event.Event;
import com.neeve.event.EventFactory;
import com.neeve.event.EventMultiplexerSingleThreaded;
import com.neeve.event.IEventHandler;
import com.neeve.event.IEventMultiplexer;
import com.neeve.tools.interactive.commands.AnnotatedCommand;
import com.neeve.util.UtlGovernor;
import com.neeve.util.UtlThread;
import com.neeve.util.UtlTime;

/**
 * Sender to benchmark event multiplexer performance
 */
@AnnotatedCommand.Command(keywords = "Driver", description = "A sender to benchmark event multiplexer performance")
final public class Driver extends AnnotatedCommand {
    final private class EventHandler implements IEventHandler {
        @Override
        final public void onEvent(final Event event) {
            final TestEvent testEvent = ((TestEvent)event);
            final int latency = (int)(System.nanoTime() - testEvent.offerTs);
            latencies[testEvent.num - 1] = latency;
            if (testEvent.num == Driver.this.count) {
                System.out.println("Writing o2p times...");
                writeLatencies("o2p.bin", Driver.this.count, latencies);
            }
        }
    }

    final private static class TestEvent extends Event {
        final static short ID = (short)10000;
        int num;
        long offerTs;

        TestEvent() {
            super(ID);
        }

        final TestEvent init(final int num) {
            super.init(null, null);
            offerTs = System.nanoTime();
            this.num = num;
            return this;
        }

        final static Event create(final int num) {
            return ((TestEvent)EventFactory.getInstance().createEvent(ID)).init(num);
        }

        final public static Event create(final Properties props) {
            return new TestEvent();
        }

        @Override
        final protected void reset() {
        }
    };

    /*
     * Configuration options
     */
    @Option(shortForm = 'n', longForm = "count", required = false, defaultValue = "15000000", description = "Number of events to send through the multiplexer")
    private int count;
    @Option(shortForm = 'r', longForm = "rate", required = false, defaultValue = "1000000", description = "Rate at which to send events through the multiplexer")
    private int rate;
    @Option(shortForm = 's', longForm = "schedule", required = false, defaultValue = "false", description = "Indicates that schedule should be used instead of multiplex to send the event")
    private boolean schedule;
    @Option(shortForm = 'o', longForm = "offerStrategy", required = false, description = "Sets the offer strategy to be configured in the multiplexer")
    private String offerStrategy;
    @Option(shortForm = 'w', longForm = "waitStrategy", required = false, description = "Sets the wait strategy to be configured in the multiplexer")
    private String waitStrategy;
    @Option(shortForm = 'q', longForm = "queueSize", required = false, description = "Sets the queue size to be configured in the multiplexer")
    private String queueSize;
    @Option(shortForm = 'p', longForm = "producerAffinity", required = false, description = "Sets the producer thread affinity")
    private String producerAffinity;
    @Option(shortForm = 'c', longForm = "consumerAffinity", required = false, description = "Sets the consumer thread affinity")
    private String consumerAffinity;
    private int[] latencies;

    public Driver() {}

    final private void writeLatencies(final String filename, final int count, final int[] latencies) {
        try {
            final FileOutputStream fos = new FileOutputStream(new File(filename));
            final DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(fos, 8192));
            for (int k = 0; k < count; k++) {
                dos.writeInt(k);
                dos.writeInt(latencies[k]);
            }
            dos.flush();
            dos.close();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    final public void execute() throws Exception {
        // register the event type
        EventFactory.getInstance().registerEventType(EventFactory.EventType.create(TestEvent.ID, "TestEvent", TestEvent.class.getName(), true, null));

        // create the multiplexer
        final Properties props = new Properties();
        if (queueSize != null) {
            props.setProperty("queueDepth", queueSize);
        }
        if (offerStrategy != null) {
            props.setProperty("queueOfferStrategy", offerStrategy);
        }
        if (waitStrategy != null) {
            props.setProperty("queueWaitStrategy", waitStrategy);
        }
        if (consumerAffinity != null) {
            props.setProperty("queueDrainerCpuAffinityMask", consumerAffinity);
        }
        final IEventMultiplexer mux = EventMultiplexerSingleThreaded.create("test", false, new EventHandler(), props);

        // dump config
        System.out.println("Configuration");
        System.out.println("  Count....,.,................" + count);
        System.out.println("  Rate.....,.,................" + rate);
        System.out.println("  Schedule...................." + schedule);
        System.out.println("  OfferStrategy..............." + offerStrategy + " (actual=" + mux.getStats().getClaimStrategy() + ")");
        System.out.println("  Wait Strategy..............." + waitStrategy + " (actual=" + mux.getStats().getWaitStrategy() + ")");
        System.out.println("  Queue Size.................." + queueSize + " (actual=" + mux.getStats().getCapacity() + ")");
        System.out.println("  Producer Affinity..........." + producerAffinity);
        System.out.println("  Consumer Affinity..........." + consumerAffinity);
        System.out.println("  nv.optimizefor.............." + (XRuntime.optimizeForThroughput() ? "Throughput" : (XRuntime.optimizeForLatency() ? "Latency" : "None")));
        System.out.println("  nv.optimizeMemoryUsage......" + XRuntime.optimizeMemoryUsage());
        System.out.println("  nv.conservecpu.............." + XRuntime.conserveCPU());
        System.out.println("  nv.enablecpuaffinitymask...." + UtlThread.cpuAffinityMasksEnabled());

        // open (start) the multiplexer
        mux.open();

        // set producer affinity
        if (producerAffinity != null) {
            UtlThread.setCPUAffinityMask(UtlThread.parseAffinityMask(producerAffinity));
        }

        // create latency array
        latencies = new int[count];
        
        // run
        try {
            final UtlGovernor governor = new UtlGovernor(rate);
            for (int i = 0 ; i < count ; i++) {
                Event event = TestEvent.create(i+1);
                try {
                    if (schedule) {
                        mux.scheduleEvent(event);
                    }
                    else {
                        mux.multiplexEvent(event, 0);
                    }
                }
                finally {
                    event.dispose(); 
                }
                governor.blockToNext();
            }
        }
        finally {
            mux.close(); 
        }
    }

    public static void main(String[] args) throws Exception {
        new Driver().run(args);
    }
}
