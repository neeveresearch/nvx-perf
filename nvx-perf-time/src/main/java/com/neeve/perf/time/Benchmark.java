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
package com.neeve.perf.time;

import java.sql.Time;
import java.util.concurrent.CountDownLatch;

import com.neeve.util.UtlConstants;
import com.neeve.util.UtlTime;

import jargs.gnu.CmdLineParser;

public class Benchmark implements Runnable {
    final private static String MODE_NATIVE = "native"; // use native wall time
    final private static String MODE_EPOCH = "epoch";   // use non-native wall time
    final private static String MODE_NANO = "nano";     // use System.nanoTime()
    final private static int WARMUPCOUNT = 10000;
    final private static int COUNT = 100000000;
    final private boolean useNanoTime;
    final private CountDownLatch startLatch;
    final private CountDownLatch joinLatch;

    Benchmark(final String mode, final CountDownLatch startLatch, final CountDownLatch joinLatch) {
        this.startLatch = startLatch;
        this.joinLatch = joinLatch;
        if (mode.equalsIgnoreCase(MODE_NATIVE)) {
            System.setProperty(UtlConstants.TIME_USENATIVE_PROPNAME, "true");
            useNanoTime = false;
        }
        else if (mode.equalsIgnoreCase(MODE_EPOCH)) {
            System.setProperty(UtlConstants.TIME_USENATIVE_PROPNAME, "false");
            useNanoTime = false;
        }
        else if (mode.equalsIgnoreCase(MODE_NANO)) {
            useNanoTime = true;
        }
        else {
            throw new IllegalArgumentException("invalid mode '" + mode + "'");
        }
    }

    final private long runUsingNanoTime() {
        System.out.println("USING System.nanoTime()");
        for (int i = 0; i < WARMUPCOUNT ; i++) {
            System.nanoTime();
        }
        long start = System.nanoTime();
        for (int i = 0 ; i < COUNT ; i++) {
            System.nanoTime();
        }
        return (System.nanoTime() - start) / COUNT;
    }

    final private long runUsingNow() {
        System.out.println("USING UtlTime.now() [NATIVE TIME IS " + (UtlTime.isNativeTimeEnabled() ? "ENABLED" : "DISABLED") + "]");
        for (int i = 0; i < WARMUPCOUNT ; i++) {
            UtlTime.now();
        }
        long start = UtlTime.now();
        for (int i = 0 ; i < COUNT ; i++) {
            UtlTime.now();
        }
        return (UtlTime.now() - start) / COUNT;
    }

    @Override
    final public void run() {
        try {
            startLatch.await();
            long val = 0;
            if (useNanoTime) {
                val = runUsingNanoTime();
            }
            else {
                val = runUsingNow();
            }
            System.out.println("[tid " + Thread.currentThread().getId() + "] Time overhead is " + val + " nanos");
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }
        finally { 
            joinLatch.countDown();
        }
    }

    final private static void printUsage() {
        System.err.println("Usage Benchmark");
        System.err.println("  [{-m, --mode} mode to use to fetch time]");
        System.err.println("    Specifies the mode used to fetch time for the benchmark. Valid options are 'native', 'epoch' and 'nano' (default='native')");
        System.err.println("  [{-t, --threads} number of concurrent threads to run]");
        System.err.println("    Specifies the number of concurrent threads to run (default=1)");
        System.err.println("  [{-h, --help} print this help string]");
    }

    // entry point
    final public static void main(final String[] args) throws Exception {
        final CmdLineParser parser = new CmdLineParser();
        final CmdLineParser.Option modeOption = parser.addStringOption('m', "mode");
        final CmdLineParser.Option numThreadsOption = parser.addIntegerOption('t', "threads");
        final CmdLineParser.Option helpOption = parser.addBooleanOption('h', "help");
        try {
            parser.parse(args);
            if (!((Boolean)parser.getOptionValue(helpOption, false))) {
                final String mode = (String)parser.getOptionValue(modeOption, MODE_NATIVE); 
                final int numThreads = (int)parser.getOptionValue(numThreadsOption, 1);
                final CountDownLatch startLatch = new CountDownLatch(1);
                final CountDownLatch joinLatch = new CountDownLatch(numThreads);
                for (int i = 0; i < numThreads ; i++) {
                    final Thread thread = new Thread(new Benchmark(mode, startLatch, joinLatch));
                    thread.setDaemon(true);
                    thread.start();
                }
                startLatch.countDown();
                joinLatch.await();
            }
            else {
                printUsage();
            }
        }
        catch (CmdLineParser.OptionException e) {
            System.err.println(e.getMessage());
            printUsage();
        }
    }
}
