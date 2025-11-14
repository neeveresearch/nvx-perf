# X Platform Performance Benchmark Suite

This repository contains performance benchmarks for all major components of the X Platform runtime. The programs serve both as performance measurement tools and as sample code demonstrating X Platform best practices.

## Canonical Benchmark

The **AEP Module** (`nvx-perf-aep`) contains the canonical end-to-end benchmark used to measure X Platform's official performance metrics published in the [X Platform Performance Documentation](https://docs.xplatform.com/performance). This benchmark exercises the complete Receive-Process-Send flow of a clustered microservice. 

## Repository Organization
This repository is organized as a multi-module Maven project. Each Maven module contains programs pertaining to a specific X runtime module. For example, the `nvx-perf-serialization` module contains programs that benchmark message serialization and deserialization, `nvx-perf-persistence` contains programs that benchmark the various message and data persisters and so on and so forth. The build process creates a single distribution containing all benchmark modules and their dependencies. 

## Modules

Each module benchmarks a specific X Platform component:

| X Platform Module    | Maven Module           | Description |
|:-------------------- |:-----------------------|:------------|
| Time                 | nvx-perf-time          | Time API overhead benchmarks |
| Encoding (ADM)       | nvx-perf-serialization | Message serialization/deserialization |
| Link                 | nvx-perf-link          | Cluster replication link throughput and latency |
| Messaging (SMA)      | nvx-perf-messaging     | Pub/sub messaging layer performance |
| Persistence          | nvx-perf-persistence   | Message and data persistence |
| Store (ODS)          | nvx-perf-storage       | Object store operations |
| Engine (AEP)         | nvx-perf-aep           | **End-to-end canonical benchmark** |

Detailed documentation for each module can be found in the [Perf Wiki](https://github.com/neeveresearch/nvx-perf/wiki).

## Versioning
A Perf release is published for each released version of the X Platform (starting with X 3.16.14). The published release has the same version as corresponding platform release. 

## Distribution Naming
The distribution is named as follows:

`nvx-perf-dist-{version}-{arch}.tar.gz` (Linux and macOS)
`nvx-perf-dist-{version}-{arch}.zip` (Windows)

Valid values for `arch` are as follows:
- linux-x86-64
- osx-x86-64
- win-x86-64

For example, the `linux-x86-64` distribution produced by the `3.16.29` perf release is named `nvx-perf-dist-3.16.29-linux-x86-64.tar.gz`

**Note**: Only Linux distributions include X Platform native libraries required for zero-garbage operation. Windows and OSX distributions can be run for development purposes, but Linux distributions are required for full performance optimization.

## Distribution Repository

Distributions can be downloaded from the Neeve artifact repository. You will need valid credentials for access.

**Download with credentials**:

```bash
wget --user=YOUR_USERNAME --password=YOUR_PASSWORD \
  https://nexus.neeveresearch.com/nexus/service/local/repositories/public-releases/content/com/neeve/nvx-perf-dist/{version}/nvx-perf-dist-{version}-{arch}.tar.gz
```

**Example** - Download the `linux-x86-64` distribution from the `3.16.29` perf release:

```bash
wget --user=YOUR_USERNAME --password=YOUR_PASSWORD \
  https://nexus.neeveresearch.com/nexus/service/local/repositories/public-releases/content/com/neeve/nvx-perf-dist/3.16.29/nvx-perf-dist-3.16.29-linux-x86-64.tar.gz
```

## Build
This section describes how to build the distribution from source. Please skip this section in case you are only interested in running tests using downloaded distributions.

### Set Up Your Environment

#### Install Maven
This repository is built using Maven. Published distributions use Maven 3.5.4. You can download the binaries for this Maven version from [here](https://archive.apache.org/dist/maven/maven-3/3.5.4/binaries/). Later versions of Maven should also work. If you are new to Maven, you can find installation instructions [here](https://maven.apache.org/index.html).

#### Install Java
This repository is built using JDK 8. You can download JDK 8 from [here](https://www.oracle.com/in/java/technologies/javase/javase8u211-later-archive-downloads.html). 

#### Set JAVA_HOME
After installing Java 8, set JAVA_HOME to the root directory of the JDK 8 installation. This will ensure that Maven picks up the installed JDK for the build.

#### Configure Maven Credentials

You need to configure credentials for the Neeve artifact repositories in your Maven settings file (`~/.m2/settings.xml`):

```xml
<settings>
  <servers>
    <server>
      <id>neeve-public</id>
      <username>YOUR_USERNAME</username>
      <password>YOUR_PASSWORD</password>
    </server>
    <server>
      <id>neeve-licensed</id>
      <username>YOUR_USERNAME</username>
      <password>YOUR_PASSWORD</password>
    </server>
  </servers>
</settings>
```

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

#### Build Distribution

To build a deployable distribution, specify a platform profile:

```bash
# For Linux (recommended for performance testing)
mvn -P linux-x86-64 clean install

# For macOS (development only)
mvn -P osx-x86-64 clean install

# For Windows (development only)
mvn -P win-x86-64 clean install
```

The distribution is created in `nvx-perf-dist/target/`.

## Running Benchmarks

### Extract Distribution

```bash
tar xvf nvx-perf-dist-{version}-{arch}.tar.gz
cd nvx-perf-{version}
```

This creates the following structure:
```
nvx-perf-{version}/
├── conf/      # Configuration files
└── libs/      # All JARs from all modules and their dependencies
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
