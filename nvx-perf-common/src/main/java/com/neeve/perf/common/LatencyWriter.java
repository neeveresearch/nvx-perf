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
package com.neeve.perf.common;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.FileInputStream;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;

import com.neeve.ci.XRuntime;
import com.neeve.quark.QuarkBuffer;

/**
 * Utility class containing methods to write performance latencies to a file
 */
final public class LatencyWriter {
    private enum State {
        Init,
        Started,
        Stopped,
        Closed;
    }

    final private class Counters {
        long start;
        long dstart;
        int trate;
        int tcount;
        int count;
        int icount;
        long total;
        long itotal;
        int max;
        long latencies;

        final void init(final int tr, final int tc) {
            dstart = start = System.nanoTime();
            trate = tr;
            tcount = tc;
            icount = count = 0;
            itotal = total = 0l;
            max = 0;
            latencies = QuarkBuffer.allocateMemoryBlock(trate * 4, true);
        }

        final boolean write(final int val, final long now) {
            QuarkBuffer.putInt(latencies, icount * 4, val);
            icount++;
            count++;
            itotal += val;
            total += val;
            if (val > max) {
                max = val;
            }
            return (icount == trate || (now - dstart) >= 1000000000l);
        }

        final void reset() {
            icount = 0;
            itotal = 0l;
            max = 0;
            dstart = System.nanoTime();
        }

        final void done() {
            if (latencies != 0l) {
                QuarkBuffer.freeMemoryBlock(latencies, trate * 4);
                latencies = 0;
            }
        }
    }

    final private String _name;
    final private String _filename;
    final private boolean _printHeader;
    final private boolean _printIntervalStats;
    final private boolean _printStatsInNanos;
    final private Counters _counters;
    private RandomAccessFile _file;
    private QuarkBuffer _mappedFile;
    private long _lw;
    private State _state;

    /**
     * Construct a latency writer 
     *  
     * @param name The name of the stats series 
     *  
     * @param filename The file to write the latencies to. Can be null if latencies are not to be written 
     *  
     * @param printHeader Whether the latencies header should be printed. This should normally be true unless 
     * multiple latency writers are being employed and only one of them should print the header 
     *  
     * @param printIntervalStats Whether to print stats on each interval that the stats are written to the 
     * file. Otherwise, only aggregate stats fot the full run are printed at the end of the run 
     *  
     * @param printStatsInNanos If true, then aggregated stats are printed in nanoseconds. Otherwise, they 
     * are printed in microseconds. This parameter does not affect what is written to the latencies file. 
     * That is always in nanoseconds i.e. what is supplied to {@link #write(int)} 
     */
    public LatencyWriter(final String name,
                         final String filename,
                         final boolean printHeader,
                         final boolean printIntervalStats,
                         final boolean printStatsInNanos) throws Exception {
        _name = name;
        _filename = filename;
        _printHeader = printHeader;
        _printIntervalStats = printIntervalStats;
        _printStatsInNanos = XRuntime.getValue("nv.perf.printlatenciesinnanos", printStatsInNanos);
        _counters = new Counters();
        _state = State.Init;
    }

    /**
     * Construct a latency writer 
     *  
     * <p> 
     * Invokes <code>this(name, filename, true, printIntervalStats, false)</code>
     */
    public LatencyWriter(final String name, final String filename, final boolean printIntervalStats) throws Exception {
        this(name, filename, true, printIntervalStats, false);
    }

    final private static int val(long arr, int idx) {
        return QuarkBuffer.getInt(arr, idx * 4);
    }

    final private static void swap(long arr, int a, int b) {
        int t = val(arr, a);
        QuarkBuffer.putInt(arr, a * 4, val(arr, b));
        QuarkBuffer.putInt(arr, b * 4, t);
    }

    /*
     *  Calculate percentile
     *  
     *  This Quickselect routine is based on the algorithm described in
     *  "Numerical recipes in C", Second Edition,
     *  Cambridge University Press, 1992, Section 8.5, ISBN 0-521-43108-5
     *  This code by Nicolas Devillard - 1998. Public domain.
     */
    final private static int percentile(long arr, int n, double rank) {
        int low, high;
        int percentile_idx;
        int middle, ll, hh;

        low = 0;
        high = n - 1;
        percentile_idx = (int)(high * (rank / 100.0));

        for (;;) {
            if (high <= low) { /* One element only */
                return val(arr, percentile_idx);
            }

            if (high == low + 1) {  /* Two elements only */
                if (val(arr, low) > val(arr, high)) {
                    swap(arr, low, high);
                }
                return val(arr, percentile_idx);
            }

            /* Find median of low, middle and high items; swap into position low */
            middle = (low + high) / 2;
            if (val(arr, middle) > val(arr, high)) {swap(arr, middle, high); }
            if (val(arr, low) > val(arr, high))    {swap(arr, low, high); }
            if (val(arr, middle) > val(arr, low))  {swap(arr, middle, low); }

            /* Swap low item (now in position middle) into position (low+1) */
            swap(arr, middle, low + 1);

            /* Nibble from each end towards middle, swapping items when stuck */
            ll = low + 1;
            hh = high;
            for (;;) {
                do ll++; while (val(arr, low) > val(arr, ll));
                do hh--; while (val(arr, hh) > val(arr, low));
                if (hh < ll) {
                    break;
                }
                swap(arr, ll, hh);
            }

            /* Swap middle item (in position 'low') back into correct position */
            swap(arr, low, hh);

            /* Re-set active partition */
            if (hh <= percentile_idx) {
                low = ll;
            }
            if (hh >= percentile_idx) {
                high = hh - 1;
            }
        }
    }

    final private void printHeader() {
        System.out.println("");
        System.out.println("+------------------------------------------------------------------------------------------------------------------------------------------+");
        if (_printStatsInNanos) {
            System.out.println("|          |              |                                          latency (nsec)                                                        |");
        }
        else {
            System.out.println("|          |              |                                          latency (usec)                                                        |");
        }
        System.out.println("+----------+--------------+--------+--------+--------+--------+----------+-----------+------------+-------------+--------+--------+--------+");
        System.out.println("|  metric  | # iterations | 50%ile | 75%ile | 90%ile | 99%ile | 99.9%ile | 99.99%ile | 99.999%ile | 99.9999%ile |   max  | avg(d) | avg(o) |");
        System.out.println("+----------+--------------+--------+--------+--------+--------+----------+-----------+------------+-------------+--------+--------+--------+");
    }

    final private void printSeparator() {
        System.out.println("+------------------------------------------------------------------------------------------------------------------------------------------+");
    }

    final private void printLatencies(final long latencies, final long itotal, final long total, final int icount, final int count, final int max) {
        if (_printStatsInNanos) {
            System.out.format(" %10s %14d %8d %8d %8d %8d %10d %11d %12d %13d %8d %8.2f %8.2f\n",
                              _name,
                              icount,
                              percentile(latencies, icount, 50.0),
                              percentile(latencies, icount, 75.0),
                              percentile(latencies, icount, 90.0),
                              percentile(latencies, icount, 99.0),
                              percentile(latencies, icount, 99.9),
                              percentile(latencies, icount, 99.99),
                              percentile(latencies, icount, 99.999),
                              percentile(latencies, icount, 99.9999),
                              max,
                              ((double)itotal) / icount,
                              ((double)total) / count);
        }
        else {
            System.out.format(" %10s %14d %8.2f %8.2f %8.2f %8.2f %10.2f %11.2f %12.2f %13.2f %8.2f %8.2f %8.2f\n",
                              _name,
                              icount,
                              ((double)percentile(latencies, icount, 50.0)) / 1000,
                              ((double)percentile(latencies, icount, 75.0)) / 1000,
                              ((double)percentile(latencies, icount, 90.0)) / 1000,
                              ((double)percentile(latencies, icount, 99.0)) / 1000,
                              ((double)percentile(latencies, icount, 99.9)) / 1000,
                              ((double)percentile(latencies, icount, 99.99)) / 1000,
                              ((double)percentile(latencies, icount, 99.999)) / 1000,
                              ((double)percentile(latencies, icount, 99.9999)) / 1000,
                              ((double)max) / 1000,
                              (((double)itotal) / icount) / 1000,
                              (((double)total) / count) / 1000);
        }
    }

    final private void printLatenciesFromFile() {
        // allocate memory to hold all the latencies in the file
        final long latencies = QuarkBuffer.allocateMemoryBlock(_counters.count * 4, true);
        try {
            // read the file
            try {
                final FileInputStream fis = new FileInputStream(_filename);
                try {
                    final BufferedInputStream bis = new BufferedInputStream(fis);
                    try {
                        final DataInputStream dis = new DataInputStream(bis);
                        int max = 0;
                        long total = 0;
                        for (int i = 0; i < _counters.count; i++) {
                            final int latency = dis.readInt();
                            QuarkBuffer.putInt(latencies, i * 4, latency);
                            total += latency;
                            if (latency > max) max = latency;
                        }
                        printLatencies(latencies, total, total, _counters.count, _counters.count, max);
                    }
                    finally {
                        bis.close();
                    }
                }
                finally {
                    fis.close();
                }
            }
            catch (Throwable e) {
                e.printStackTrace();
            }
        }
        finally {
            QuarkBuffer.freeMemoryBlock(latencies, _counters.count * 4);
        }
    }

    /**
     * Process a set of collected latencies 
     */
    final private void process() throws Exception {
        // get counters
        final long latencies = _counters.latencies;
        final long itotal = _counters.itotal;
        final long total = _counters.total;
        final int icount = _counters.icount;
        final int count = _counters.count;
        final int max = _counters.max;

        // print latencies if configured to so
        if (_printIntervalStats) {
            printLatencies(latencies, itotal, total, icount, count, max);
        }

        // write to file if configured to do so
        if (_mappedFile != null) {
            for (int i = 0; i < icount; i++) {
                QuarkBuffer.putInt(latencies, i * 4, Integer.reverseBytes(QuarkBuffer.getInt(latencies, i * 4)));
            }
            QuarkBuffer.copy(latencies,
                             0,
                             _mappedFile.getNativeAddress(),
                             (count - icount) * 4,
                             icount * 4);
        }
    }

    /**
     * Start a latency writer 
     *  
     * @param rate The rate at which points will be generated 
     *  
     * @param count The total number of data points
     */
    final public void start(final int rate, final int count) throws Exception {
        // validate state
        if (_state != State.Init) throw new IllegalStateException("illegal state '" + _state + "'");

        // initialize counter
        _counters.init(rate, count);

        // no, open the latencies file if specified
        if (_filename != null) {
            _file = new RandomAccessFile(_filename, "rw");
            _file.setLength(0);
            _file.setLength(count * 4);
            _mappedFile = QuarkBuffer.wrap(_file.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, _file.length()));
        }

        // print header if specfied and interval stats to be printed (otherwise, header is printed later at end of run)
        if (_printHeader && _printIntervalStats) printHeader();

        // update state
        _state = State.Started;
    }

    /**
     * Write a latency data point 
     *  
     * @param val The data point, in nanoseconds
     *  
     * @return Returns if the write resulted in the filling up of the latencies 
     * buffer and processing/writing of the buffer as configured during start
     */
    final public boolean write(final int val) throws Exception {
        // validate state
        if (_state != State.Started) throw new IllegalStateException("illegal state '" + _state + "'");

        // add to counters and process stored set on write to file
        final long now = System.nanoTime();
        final boolean processed;
        if (processed = _counters.write(val, now)) {
            process();
            _counters.reset();
        }

        // return if processed (written to file)
        return processed;
    }

    /**
     * Stop a latency writer 
     */
    final public void stop() throws Exception {
        // validate state
        if (_state == State.Stopped) return;
        if (_state != State.Started) throw new IllegalStateException("illegal state '" + _state + "'");

        // process remainder latencies
        if (_counters.icount > 0) {
            process();
        }

        // update state
        _state = State.Stopped;
    }

    /**
     * Close a latency writer 
     *  
     * @param finish Whether the latency writing is finished
     */
    final public void close(final boolean finish) throws Exception {
        // validate state
        if (_state == State.Closed) return;

        try {
            // stop
            stop();

            // latencies written to a file?
            if (_filename != null) {
                // release file resources
                // ...do this now so that all buffered data is flushed to the file
                _mappedFile.dispose();
                _file.close();

                // yes, if print header is configured, then print header/separator
                if (_printHeader) {
                    // print header if interval stats are not enabled i.e. the header was
                    // not printed at start. otherwise, print just the separator to separate
                    // the interval latencies for the full run
                    if (!_printIntervalStats) {
                        printHeader();
                    }
                    else {
                        printSeparator();
                    }
                }

                // print latencies for full run
                printLatenciesFromFile();
            }

            // finally, if print header is configured, then print separator
            if (finish && _printHeader && (_filename != null || _printIntervalStats)) {
                printSeparator();
                System.out.println("");
            }

            // free counter resources
            _counters.done();
            
        }
        finally {
            _state = State.Closed;
        }
    }

    /**
     * Close a latency writer 
     *  
     * <p> 
     * Invokes <code>close(true)</code> 
     * </p> 
     */
    final public void close() throws Exception {
        close(true);
    }

    /**
     * Print finish line
     */
    final public void finish() {
        printSeparator();
    }
}
