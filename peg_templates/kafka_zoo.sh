#!/bin/bash

CLUSTER_NAME=ao-kafka

peg up kafka.yml &

wait

peg fetch ${CLUSTER_NAME}

peg install ${CLUSTER_NAME} zookeeper
peg install ${CLUSTER_NAME} kafka

