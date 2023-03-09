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
package com.neeve.perf.serialization;

import com.neeve.stats.Stats.LatencyManager;
import com.neeve.tools.interactive.commands.AnnotatedCommand;

@AnnotatedCommand
.Command(keywords = "SerializationDriver", description = "A driver to benchmark serialization deserialization times")
public class Driver extends AnnotatedCommand {
    @Option(shortForm = 'p', longForm = "provider", defaultValue = "xbuf2", description = "The serialization provider to use")
    private String providerStr;

    private void perfTestEncode(final Provider provider, final int runNumber, final LatencyManager latencyManager, final int reps, final long nanoTimeOverhead) {
        latencyManager.reset();
        for (int i = 0; i < reps; i++) {
            provider.prepareToEncode();
            final long encodeStart = System.nanoTime();
            provider.encode();
            final long encodeTime = System.nanoTime() - encodeStart - nanoTimeOverhead;
            provider.postEncode();
            latencyManager.add(encodeTime);
        }
        latencyManager.compute();
        System.out.printf("%-25s %-3d %-6s %-5d %-5.0f %-5.0f%n",
                          provider.name(),
                          runNumber,
                          "ENC",
                          provider.encodedLength(),
                          latencyManager.median(),
                          latencyManager.mean());
    }

    private void perfTestDecode(final Provider provider, final int runNumber, final LatencyManager latencyManager, final int reps, final long nanoTimeOverhead) {
        latencyManager.reset();
        for (int i = 0; i < reps; i++) {
            provider.prepareToDecode();
            final long decodeStart = System.nanoTime();
            provider.decode();
            final long decodeTime = System.nanoTime() - decodeStart - nanoTimeOverhead;
            provider.postDecode();
            latencyManager.add(decodeTime);
        }
        latencyManager.compute();
        System.out.printf("%-25s %-3d %-6s %-5d %-5.0f %-5.0f%n",
                          provider.name(),
                          runNumber,
                          "DEC",
                          provider.decodedLength(),
                          latencyManager.median(),
                          latencyManager.mean());
    }

    @Override
    public void execute() {
        Provider provider;
        if (providerStr.equalsIgnoreCase("rumi.xbuf2") || providerStr.equalsIgnoreCase("rumi.xbuf2.serial")) {
            provider = new com.neeve.perf.serialization.rumi.xbuf2.serial.CarBenchmark();
        }
        else if (providerStr.equalsIgnoreCase("rumi.xbuf2.random")) {
            provider = new com.neeve.perf.serialization.rumi.xbuf2.random.CarBenchmark();
        }
        else {
            throw new IllegalArgumentException("invalid serialization provider '" + providerStr + "'");
        }
        final int reps = 10 * 1000 * 1000;
        final LatencyManager latencyManager = new LatencyManager("encdec", reps);
        long nanoTimeOverhead = 0l;
        long start = System.nanoTime();
        System.out.println("Calculating nanoTime() overhead...");
        for (int i = 0; i < 100000000; i++) {
            System.nanoTime();
        }
        nanoTimeOverhead = (System.nanoTime() - start) / 100000000;
        System.out.println("..." + nanoTimeOverhead + " nanos");
        System.out.printf("%-25s %-3s %-6s %-5s %-5s %-5s%n",
                          "PROV",
                          "RUN",
                          "ENCDEC",
                          "LEN",
                          "MED",
                          "AVG");
        for (int i = 0; i < 10; i++) {
            perfTestEncode(provider, i, latencyManager, reps, nanoTimeOverhead);
            perfTestDecode(provider, i, latencyManager, reps, nanoTimeOverhead);
        }
    }

    public static void main(final String[] args) {
        System.getProperties().setProperty("nv.optimizefor", "latency");
        try {
            new Driver().run(args);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
}
