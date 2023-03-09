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

import java.util.Properties;

public class SystemProperties {
    public static void dump() {
        Properties props = System.getProperties();
        System.out.println("OS Name          = " + props.getProperty("os.name"));
        System.out.println("OS Arch          = " + props.getProperty("os.arch"));
        System.out.println("OS Version       = " + props.getProperty("os.version"));
        System.out.println("JRE Spec Name    = " + props.getProperty("java.specification.name"));
        System.out.println("JRE Spec Version = " + props.getProperty("java.specification.version"));
        System.out.println("JRE Spec Vendor  = " + props.getProperty("java.specification.vendor"));
        System.out.println("JRE Version      = " + props.getProperty("java.version"));
        System.out.println("JRE Vendor       = " + props.getProperty("java.vendor"));
        System.out.println("JRE Home         = " + props.getProperty("java.home"));
        System.out.println("JVM Spec Name    = " + props.getProperty("java.vm.specification.name"));
        System.out.println("JVM Spec Version = " + props.getProperty("java.vm.specification.version"));
        System.out.println("JVM Spec Vendor  = " + props.getProperty("java.vm.specification.vendor"));
        System.out.println("JVM Impl Name    = " + props.getProperty("java.vm.name"));
        System.out.println("JVM Impl Version = " + props.getProperty("java.vm.version"));
        System.out.println("JVM Impl Vendor  = " + props.getProperty("java.vm.vendor"));
        System.out.println("Java Class Path  = " + props.getProperty("java.class.path"));
        System.out.println("JIT Compiler     = " + props.getProperty("java.compiler"));
    }
}
