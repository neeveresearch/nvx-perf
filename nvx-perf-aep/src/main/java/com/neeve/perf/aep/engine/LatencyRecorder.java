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
package com.neeve.perf.aep.engine;

import com.neeve.perf.common.LatencyWriter;
import com.neeve.util.UtlTime;

final public class LatencyRecorder {
    final private static long _utlTimeOverhead;
    private static boolean _noWrite;
    private static boolean _printIntervalStats;
    private static LatencyWriter _lw_w2w;

    static {
        try {
            // calc UtlTime.now() overhead
            long start = UtlTime.now();
            for (int i = 0; i < 100000000l; i++) {
                UtlTime.now();
            }
            _utlTimeOverhead = (UtlTime.now() - start) / 100000000l;
            System.out.println("UtlTime overhead=" + _utlTimeOverhead + "ns");
        }
        catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

    final public static void noWrite(final boolean val) {
        _noWrite = val;
    }

    final public static void printIntervalStats(final boolean val) {
        _printIntervalStats = val;
    }

    final public static void start(final int rate, final int count) throws Exception {
        (_lw_w2w = new LatencyWriter("w2w", _noWrite ? null : "latencies.w2w.bin", _printIntervalStats)).start(rate, count);
    }

    final public static void recordW2w(final long val) throws Exception {
        _lw_w2w.write((int)(val - _utlTimeOverhead));
    }

    final public static void stop() throws Exception {
        _lw_w2w.close();
    }
}

