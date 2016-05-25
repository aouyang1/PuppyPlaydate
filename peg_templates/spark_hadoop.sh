#!/bin/bash

CLUSTER_NAME=ao-spark

peg up spark_master.yml &
peg up spark_workers.yml &

wait

peg fetch ${CLUSTER_NAME}

peg install ${CLUSTER_NAME} ssh
peg install ${CLUSTER_NAME} aws
peg install ${CLUSTER_NAME} hadoop
peg install ${CLUSTER_NAME} spark

peg scp to-rem ${CLUSTER_NAME} 1 ~/.ssh/id_rsa /home/ubuntu/.ssh/
peg sshcmd ${CLUSTER_NAME} 1 "ssh-add ~/.ssh/id_rsa"
peg sshcmd ${CLUSTER_NAME} 1 "ssh-keyscan -H -t rsa github.com >> ~/.ssh/known_hosts"
peg sshcmd ${CLUSTER_NAME} 1 "git clone git@github.com:aouyang1/PuppyPlaydate.git ~/puppy"

