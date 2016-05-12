#!/bin/bash

CLUSTER_NAME=cassandra-ao

peg up cassandra.yml &

wait

peg fetch ${CLUSTER_NAME}

peg install ${CLUSTER_NAME} ssh
peg install ${CLUSTER_NAME} cassandra
peg install ${CLUSTER_NAME} opscenter
