#!/bin/bash

git pull
lein do clean, compile, uberjar
scp target/knowing-hadoop-0.1.0-SNAPSHOT-standalone.jar 10.10.6.99:/tmp
