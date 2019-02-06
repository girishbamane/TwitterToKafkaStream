# TwitterToKafkaStream
$SPARK_HOME/bin/spark-submit --class kafka.twitter.flow.TwitterToKafkaStream --master spark://orienit:7077 /home/orienit/Desktop/WordCount/demo/TwitterTokafkaTopicStream.jar ~/Desktop/WordCount/input.txt ~/Desktop/WordCount/output1

bin\windows\kafka-run-class.bat kafka.tools.GetOffsetShell --broker-list localhost:9092 --topic twitter_topicv6
