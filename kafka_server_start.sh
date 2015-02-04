#!/bin/sh

SESSION_NAME="kafka_server"

tmux new-session -s ${SESSION_NAME} -n bash -d

tmux send-keys -t ${SESSION_NAME} 'sudo ~/Downloads/kafka_2.9.2-0.8.1.1/bin/kafka-server-start.sh ~/Downloads/kafka_2.9.2-0.8.1.1/config/server.properties' C-m

