echo 'Clean existing topic...'
$KAFKA_HOME/bin/kafka-topics.sh --zookeeper localhost:2181 --delete --topic police.department.calls

echo 'Stop Kafka server'
$KAFKA_HOME/bin/kafka-server-stop.sh

echo 'Stop Zookeeper server'
$KAFKA_HOME/bin/zookeeper-server-stop.sh
