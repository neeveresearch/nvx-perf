# The Perf Repository

This repository contains programs to run performance benchmarks on the various modules that comprise the X Platform runtime. In addition, these programs also serve as sample programs illustrating how to program to these modules. 

## Repository Organization
This repository is organized as a multi-module Maven project. Each Maven module contains programs pertaining to a specific X runtime module. For example, the `nvx-perf-serialization` module contains programs that benchmark message serialization and deserialization, `nvx-perf-persistence` contains programs that benchmark the various message and data persisters and so on and so forth. Each Maven module generates an independently deployable distribution as part of the build process. To run tests pertaining to a particular module, you can either download the published distribution for the module from the Neeve artifact repository or build and deploy the module's distribution and run the desired tests. 

## Modules
The following are the various modules that produce deployable distributions

| X Platform Module    | Maven Module           |
|:-------------------- |:-----------------------|
| Time                 | nvx-perf-time          |
| Encoding (ADM)       | nvx-perf-serialization |
| Link                 | nvx-perf-link          |
| Messaging (SMA)      | nvx-perf-messaging     |
| Persistence          | nvx-perf-persistence   |
| Store (ODS)          | nvx-perf-storage       |
| Engine (AEP)         | nvx-perf-aep           |

More detailed information about each of these modules can be found on the Perf wiki.

## Versioning
A Perf release is published for each released version of the X Platform starting with 3.16.14 using the same version as the platform release. Built module distributions for each Perf release are built and published to the Neeve artifact repository from where they can be downloaded. The following is the 

## Distribution Naming
A module distribution is named as follows

`nvx-perf-{module}-{version}-dist-{arch}.tar.gz`

Valid values for `arch` are as follows:
- linux-x86-64
- osx-x86-64
- win-x86-64

For example, the distribution for the `persistence` module for `linux-x86-64` architecture produced by the `3.16.29` perf relesed is named `nvx-perf-aep-3.16.29-dist-linux-x86-64.tar.gz`

```
Note: Only the Linux distributions contain the X Platform native libraries some of which are needed for zero garbage operation of the platform. Therefore, although the Windows and OSX distributions can be run, as of now it is only the Linux distributions that are fully optimized for performance
```

## Download Distributions
Built distributions for published Perf releases (corresponding to X Platform releases) can be downloaded from the Neeve artifact repository as follows:

`wget http://nexus.rumidata.io:8081/repository/maven-public/com/neeve/nvx-perf-{module}/{version}/nvx-perf-{module}-{version}-dist-{arch}.tar.gz`

For example, execute the following to download the distribution for the `persistence` module for `linux-x86-64` architecture produced by the `3.16.29` perf release

`wget http://nexus.rumidata.io:8081/repository/maven-public/com/neeve/nvx-perf-persistence/3.16.29/nvx-perf-persistence-3.16.29-dist-linux-x86-64.tar.gz`

## Build Distributions
This section describes how to build the module distributions from source. Please skip this section in case you are only interested in running tests using downloaded distributions.

### Set Up Your Environment

#### Install Maven
This repository is built using Maven. Published distributions of the modules in this repository are built using Maven 3.5.4. To build, you You can download the binaries for this Maven version from [here](https://archive.apache.org/dist/maven/maven-3/3.5.4/binaries/). Feel free to use later versions of Maven if you need to. If you are new to Maven, you can find instructions [here](https://maven.apache.org/index.html) on how to install and configure Maven.

#### Install Java
This repository is built using JDK 8. You can download JDK 8 from [here](https://www.oracle.com/in/java/technologies/javase/javase8u211-later-archive-downloads.html). 

#### Set JAVA_HOME
After installing Java 8, set JAVA_HOME to the root directory of the JDK 8 installation. This will ensure that Maven picks up the installed JDK for the build.

#### X Platform License
You do NOT need an X Platform license to build or run the module distributions. The built distributions come with an embedded version of the license. 

### Build

#### Clone The Repository
- Ensure the `git` - the command line Git client - is installed on your machine
- Open a terminal window
- Execute `git clone https://github.com/neeveresearch/nvx-perf.git` to clone the repository

There are several other techniques to clone the repository. Feel free to use any technique that works for you.

#### Build The Repository Modules
- Go to the base directory of the cloned repository
- Execute `mvn install`

The above will build all the modules but will _not_ build the module's deployable distributions. 

#### Build The Repository Module Distributions
- Go to the base directory of the cloned repository
- Execute `mvn -P <arch> clean install`

## Run
The following are the general steps of how one would run tests contained in a module's distribution

#### Copy
Copy the module distribution to the target machine(s) where the tests will be executed

#### Unarchive
Execute `tar xvf <distribution>` to unarchive the distribution. This will result in the following folder structure
```
|
|---conf
|---libs
```

#### Run Test
Execute a performance benchmark as follows:

`{JAVA_HOME_OF_CHOICE}/bin/java -cp "libs/*" {Performance Program} {Program Parameters}`

## Next Steps
Detailed information about each of the perf modules, the test programs contained in each module and various parameters to those tests can be found in the Perf Wiki
Detailed information about the 
