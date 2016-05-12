#!/bin/bash

CLUSTER_NAME=kafka-ao

peg up kafka.yml &

wait

peg fetch ${CLUSTER_NAME}

peg install ${CLUSTER_NAME} zookeeper
peg install ${CLUSTER_NAME} kafka
