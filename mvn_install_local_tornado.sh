#!/bin/bash
##
## ckatsak, Mon Nov  2 17:18:40 EET 2020

set -eux

## Path to maven executable
MVN=/opt/apache-maven-3.6.3/bin/mvn

## The base directory where TornadoVM JARs live
BASEDIR=/opt/ckatsak/tornado-temp/tornado
#BASEDIR=/home/users/kbitsak/workspace/tornado_installation/tornadovm_flink/dist/tornado-sdk/tornado-sdk-0.7-6de113e/share/java/tornado

## TornadoVM's version
TORNADO_VERSION=0.7

## Install HAIER's four dependencies of TornadoVM in the local maven repo off
## the local filesystem
targets=( tornado-api tornado-matrices tornado-drivers-opencl )
for target in "${targets[@]}"; do
	$MVN install:install-file \
		-Dfile=${BASEDIR}/${target}-${TORNADO_VERSION}.jar \
		-DgroupId=tornado \
		-DartifactId=${target} \
		-Dversion=${TORNADO_VERSION} \
		-Dpackaging=jar
done
$MVN install:install-file \
	-Dfile=${BASEDIR}/tornado-drivers-opencl-jni-${TORNADO_VERSION}-libs.jar \
	-DgroupId=tornado \
	-DartifactId=tornado-drivers-opencl-jni \
	-Dversion=${TORNADO_VERSION} \
	-Dpackaging=jar
