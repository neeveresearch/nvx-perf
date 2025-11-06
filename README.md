# X Platform Performance Benchmark Suite

This repository contains performance benchmarks for all major components of the X Platform runtime. The programs serve both as performance measurement tools and as sample code demonstrating X Platform best practices.

## Canonical Benchmark

The **AEP Module** (`nvx-perf-aep`) contains the canonical end-to-end benchmark used to measure X Platform's official performance metrics published in the [X Platform Performance Documentation](https://docs.xplatform.com/x-platform/performance). This benchmark exercises the complete Receive-Process-Send flow of a clustered microservice. 

## Repository Organization
This repository is organized as a multi-module Maven project. Each Maven module contains programs pertaining to a specific X runtime module. For example, the `nvx-perf-serialization` module contains programs that benchmark message serialization and deserialization, `nvx-perf-persistence` contains programs that benchmark the various message and data persisters and so on and so forth. Each Maven module generates an independently deployable distribution as part of the build process. To run tests pertaining to a particular module, you can either download the published distribution for the module from the Neeve artifact repository or build and deploy the module's distribution and run the desired tests. 

## Modules

Each module benchmarks a specific X Platform component and produces an independently deployable distribution:

| X Platform Module    | Maven Module           | Description |
|:-------------------- |:-----------------------|:------------|
| Time                 | nvx-perf-time          | Time API overhead benchmarks |
| Encoding (ADM)       | nvx-perf-serialization | Message serialization/deserialization |
| Link                 | nvx-perf-link          | Low-level transport throughput and latency |
| Messaging (SMA)      | nvx-perf-messaging     | Pub/sub messaging layer performance |
| Persistence          | nvx-perf-persistence   | Message and data persistence |
| Store (ODS)          | nvx-perf-storage       | Object store operations |
| Engine (AEP)         | nvx-perf-aep           | **End-to-end canonical benchmark** |

Detailed documentation for each module can be found in the [Perf Wiki](https://github.com/neeveresearch/nvx-perf/wiki).

## Versioning
A Perf release is published for each released version of the X Platform (starting with X 3.16.14). The published release has the same version as corresponding platform release. 

## Distribution Naming
A module distribution is named as follows

`nvx-perf-{module}-{version}-dist-{arch}.tar.gz`

Valid values for `arch` are as follows:
- linux-x86-64
- osx-x86-64
- win-x86-64

For example, the distribution for the `persistence` module for `linux-x86-64` architecture produced by the `3.16.29` perf release is named `nvx-perf-persistence-3.16.29-dist-linux-x86-64.tar.gz`

**Note**: Only Linux distributions include X Platform native libraries required for zero-garbage operation. Windows and OSX distributions can be run for development purposes, but Linux distributions are required for full performance optimization.

## Distribution Repository
Distributions can be downloaded from the Neeve artifact repository as follows:

`wget http://nexus.rumidata.io:8081/repository/maven-public/com/neeve/nvx-perf-{module}/{version}/nvx-perf-{module}-{version}-dist-{arch}.tar.gz`

For example, execute the following to download the distribution for the `persistence` module for `linux-x86-64` architecture produced by the `3.16.29` perf release

`wget http://nexus.rumidata.io:8081/repository/maven-public/com/neeve/nvx-perf-persistence/3.16.29/nvx-perf-persistence-3.16.29-dist-linux-x86-64.tar.gz`

## Build
This section describes how to build the module distributions from source. Please skip this section in case you are only interested in running tests using downloaded distributions.

### Set Up Your Environment

#### Install Maven
This repository is built using Maven. Published distributions use Maven 3.5.4. You can download the binaries for this Maven version from [here](https://archive.apache.org/dist/maven/maven-3/3.5.4/binaries/). Later versions of Maven should also work. If you are new to Maven, you can find installation instructions [here](https://maven.apache.org/index.html).

#### Install Java
This repository is built using JDK 8. You can download JDK 8 from [here](https://www.oracle.com/in/java/technologies/javase/javase8u211-later-archive-downloads.html). 

#### Set JAVA_HOME
After installing Java 8, set JAVA_HOME to the root directory of the JDK 8 installation. This will ensure that Maven picks up the installed JDK for the build.

#### X Platform License
You do not need an X Platform license to build or run the module distributions. The distributions include an embedded license. 

### Build

#### Clone The Repository

```bash
git clone https://github.com/neeveresearch/nvx-perf.git
cd nvx-perf
```

#### Build The Repository Modules

```bash
mvn clean install
```

This builds all modules but does not create deployable distributions.

#### Build Module Distributions

To build deployable distributions, specify a platform profile:

```bash
# For Linux (recommended for performance testing)
mvn -P linux-x86-64 clean install

# For macOS (development only)
mvn -P osx-x86-64 clean install

# For Windows (development only)
mvn -P win-x86-64 clean install
```

Distributions are created in each module's `target/` directory.

## Running Benchmarks

### Extract Distribution

```bash
tar xvf nvx-perf-{module}-{version}-dist-{arch}.tar.gz
cd nvx-perf-{module}-{version}
```

This creates the following structure:
```
nvx-perf-{module}-{version}/
├── conf/      # Configuration files
└── libs/      # All dependencies
```

### Run a Benchmark

```bash
$JAVA_HOME/bin/java -cp "libs/*" {BenchmarkClass} {parameters}
```

**Example** - Run serialization benchmark:
```bash
$JAVA_HOME/bin/java -cp "libs/*" com.neeve.perf.serialization.Driver --provider xbuf2.random
```

See the [Perf Wiki](https://github.com/neeveresearch/nvx-perf/wiki) for specific benchmark classes and parameters for each module.

## Next Steps
Detailed information about each of the perf modules, the test programs contained in each module and various parameters to those tests can be found in the [Perf Wiki](https://github.com/neeveresearch/nvx-perf/wiki). 
