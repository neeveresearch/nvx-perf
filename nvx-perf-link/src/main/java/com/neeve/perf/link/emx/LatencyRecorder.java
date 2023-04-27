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
package com.neeve.perf.link.emx;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import com.neeve.quark.QuarkBuffer;

class LatencyRecorder {
    final long buffer;
    int count;

    LatencyRecorder(final int maxCount) {
        buffer = QuarkBuffer.allocateMemoryBlock(maxCount * 4, true);
    }

    void record(int val) {
        QuarkBuffer.putInt(buffer, count++ * 4, val);
    }

    void write(final String filenameQualifier) throws IOException {
        final FileOutputStream fos = new FileOutputStream(new File("latencies." + filenameQualifier + ".bin"));
        final DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(fos, 8192));
        for (int i = 0; i < count; i++) {
            dos.writeInt(i);
            dos.writeInt(QuarkBuffer.getInt(buffer, i * 4));
        }
        dos.flush();
        dos.close();
    }
}
