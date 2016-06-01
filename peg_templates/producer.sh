#!/bin/bash

CLUSTER_NAME=ao-producer

peg up producer.yml

peg fetch ${CLUSTER_NAME}

peg scp to-rem ${CLUSTER_NAME} 1 ~/.ssh/id_rsa /home/ubuntu/.ssh/
peg sshcmd ${CLUSTER_NAME} 1 "ssh-add ~/.ssh/id_rsa"
peg sshcmd ${CLUSTER_NAME} 1 "ssh-keyscan -H -t rsa github.com >> ~/.ssh/known_hosts"
peg sshcmd ${CLUSTER_NAME} 1 "git clone git@github.com:aouyang1/PuppyPlaydate.git ~/puppy"

