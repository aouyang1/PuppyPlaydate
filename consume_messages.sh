SESSION_NAME="kafka_consumer"

tmux new-session -s ${SESSION_NAME} -n bash -d
tmux send-keys -t ${SESSION_NAME} 'python ingestion/kafka_to_hdfs_messages.py ' C-m

