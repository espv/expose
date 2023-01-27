#!/bin/bash
# Use to place Expose as a local maven module which SPE wrappers can use as dependency
mvn clean package && \
	mvn install:install-file \
	-Dfile=target/experimentframework-1.0.jar \
	-DgroupId=no.uio.ifi \
	-DartifactId=experimentframework \
	-Dversion=1.0 \
	-Dpackaging=jar \
	-DgeneratePom=true
