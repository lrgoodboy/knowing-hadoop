#!/bin/bash

git pull
rm -fr ~/.m2/repository/clojure-hadoop
lein do clean, compile, uberjar
scp target/knowing-hadoop-0.1.0-SNAPSHOT-standalone.jar xapp10-127:/tmp/knowing-hadoop.jar
