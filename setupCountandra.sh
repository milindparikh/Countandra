#!/bin/bash
cassandraHostIp=`sed '/^\#/d' config/countandra.properties | grep 'cassandraHostIp'  | tail -n 1 | cut -d "=" -f2- | sed 's/^[[:space:]]*//;s/[[:space:]]*$//'` 
cassandraPort=`sed '/^\#/d' config/countandra.properties | grep 'cassandraPort'  | tail -n 1 | cut -d "=" -f2- | sed 's/^[[:space:]]*//;s/[[:space:]]*$//'` 
cassandraDirectory=`sed '/^\#/d' config/countandra.properties | grep 'cassandraDirectory'  | tail -n 1 | cut -d "=" -f2- | sed 's/^[[:space:]]*//;s/[[:space:]]*$//'` 
cassandraListenAddress=`sed '/^\#/d' config/countandra.properties | grep 'cassandraListenAddress'  | tail -n 1 | cut -d "=" -f2- | sed 's/^[[:space:]]*//;s/[[:space:]]*$//'` 
cassandraRPC=`sed '/^\#/d' config/countandra.properties | grep 'cassandraRPC'  | tail -n 1 | cut -d "=" -f2- | sed 's/^[[:space:]]*//;s/[[:space:]]*$//'` 
cassandraSeeds=`sed '/^\#/d' config/countandra.properties | grep 'cassandraSeeds'  | tail -n 1 | cut -d "=" -f2- | sed 's/^[[:space:]]*//;s/[[:space:]]*$//'` 
cat build.xml.orig | sed -e "s#__cassandraHostIp__#$cassandraHostIp#g" -e "s#__cassandraPort__#$cassandraPort#g" -e "s#__cassandraDirectory__#$cassandraDirectory#g" >build.xml
cat config/cassandra.yaml.orig | sed -e "s#__cassandraHostIp__#$cassandraHostIp#g" -e "s#__cassandraPort__#$cassandraPort#g" -e "s#__cassandraDirectory__#$cassandraDirectory#g" -e "s#__cassandraSeeds__#$cassandraSeeds#g">config/cassandra.yaml
