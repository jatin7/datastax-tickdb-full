#!/bin/bash
clear
echo "Running Server"

cd datastax-tickdb

mvn clean compile package -DskipTests

mvn jetty:run -Djetty.port=7001
