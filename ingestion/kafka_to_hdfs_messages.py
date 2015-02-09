__author__ = 'aouyang1'
import time
from kafka.client import KafkaClient
from kafka.consumer import SimpleConsumer
import os


class Consumer(object):

    def __init__(self, addr, group, topic):
        self.client = KafkaClient(addr)
        self.consumer = SimpleConsumer(self.client, group, topic,
                                       max_buffer_size=1310720000)
        self.temp_file_path = None
        self.temp_file = None
        self.topic = topic
        self.group = group
        self.block_cnt = 0


    def consume_topic(self, output_dir):

        timestamp = time.strftime('%Y%m%d%H%M%S')
        
        #open file for writing
        self.temp_file_path = "/home/ubuntu/PuppyPlaydate/ingestion/kafka_messages/kafka_%s_%s_%s.dat" % (self.topic, self.group, timestamp)
        self.temp_file = open(self.temp_file_path,"w")
        log_has_at_least_one = False #did we log at least one entry?

        while True:
            try:
                #get 1000 messages at a time, non blocking
                messages = self.consumer.get_messages(count=1000, block=False)

                #OffsetAndMessage(offset=43, message=Message(magic=0,
                # attributes=0, key=None, value='some message'))
                for message in messages:
                    log_has_at_least_one = True
                    self.tempfile.write(message.message.value + "\n")

                #file size > 20MB
                if self.tempfile.tell() > 20000000:
                    self.flush_to_hdfs(output_dir)

                self.consumer.commit()
            except:
                self.consumer.seek(0, 2)

        # exit loop
        if log_has_at_least_one:
            self.flush_to_hdfs(output_dir, self.topic)

        #save position in the kafka queue
        self.consumer.commit()

    def flush_to_hdfs(self, output_dir):
        self.tempfile.close()

        timestamp = time.strftime('%Y%m%d%H%M%S')

        hadoop_path = "/user/PuppyPlaydate/history/%s_%s_%s.dat" % (self.group, self.topic, timestamp)
        cached_path = "/user/PuppyPlaydate/cached/%s_%s_%s.dat" % (self.group, self.topic, timestamp)
        print "Block " + str(self.block_cnt) + ": Flushing 20MB file to HDFS => " + hadoop_path
        self.block_cnt += 1

        # place blocked messages into history and cached folders on hdfs
        os.system("sudo -u hdfs hdfs dfs -put %s %s" % (self.temp_file_path,
                                                        hadoop_path))
        os.system("sudo -u hdfs hdfs dfs -put %s %s" % (self.temp_file_path,
                                                        cached_path))
        os.remove(self.temp_file_path)

        timestamp = time.strftime('%Y%m%d%H%M%S')

        self.temp_file_path = "/home/ubuntu/PuppyPlaydate/ingestion/kafka_messages/kafka_%s_%s_%s.dat" % (self.topic, self.group, timestamp)
        self.temp_file = open(self.temp_file_path, "w")


if __name__ == '__main__':
    group = "hdfs"
    output = "/data"
    topic = "hdfs"
    
    print "\nConsuming topic: [%s] into HDFS" % topic
    cons = Consumer(addr="localhost:9092", group="hdfs", topic="messages")
    cons.consume_topic("user/PuppyPlaydate")
