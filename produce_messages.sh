#!/bin/sh

SESSION_NAME="kafka_producer"

tmux new-session -s ${SESSION_NAME} -n bash -d
tmux send-keys -t ${SESSION_NAME} 'python ingestion/kafka_producer_messages.py ' + $1 C-m


