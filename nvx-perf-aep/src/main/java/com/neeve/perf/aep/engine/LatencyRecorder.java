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

import com.neeve.config.Config;
import com.neeve.stats.StatsLatencyWriter;
import com.neeve.util.UtlTime;

final public class LatencyRecorder {
    public enum LegToRecord {
        w2w,
        w2b,
        b,
        s;
    }
    final private static LegToRecord _leg;
    final private static StatsLatencyWriter _lw;
    final private static long _utlTimeOverhead;

    static {
        try {
            _leg = LegToRecord.valueOf(Config.getValue("processor.latencyLegToRecord", "w2w"));
            _lw = new StatsLatencyWriter("latencies.bin");
            long nanoTimeOverhead = 0l;
            long start = UtlTime.now();
            for (int i = 0; i < 100000000l; i++) {
                UtlTime.now();
            }
            _utlTimeOverhead = (UtlTime.now() - start) / 100000000l;
            System.out.println("*** Latency Leg To Record=" + _leg);
            System.out.println("*** UtlTime overhead=" + _utlTimeOverhead + "ns");
            System.out.println("*** LatencyWriter useNative=" + StatsLatencyWriter.isNativeEnabled());
        }
        catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

    final public static LegToRecord legToRecord() {
        return _leg;
    }

    final public static void start(final int rate, final int count) throws Exception {
        _lw.start(rate, count, true, true);
    }

    final public static void recordW2w(final long val) throws Exception {
        if (_leg == LegToRecord.w2w) {
            _lw.write((int)(val - 5 * _utlTimeOverhead));
        }
    }

    final public static void recordW2b(final long val) throws Exception {
        if (_leg == LegToRecord.w2b) {
            _lw.write((int)(val - 1 * _utlTimeOverhead));
        }
    }

    final public static void recordB(final long val) throws Exception {
        if (_leg == LegToRecord.b) {
            _lw.write((int)(val - 2 * _utlTimeOverhead));
        }
    }

    final public static void recordS(final long val) throws Exception {
        if (_leg == LegToRecord.s) {
            _lw.write((int)(val - 1 * _utlTimeOverhead));
        }
    }

    final public static void stop() throws Exception {
        _lw.close();
    }
}

