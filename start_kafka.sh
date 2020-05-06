echo 'Start Zookeeper server as daemon'
$KAFKA_HOME/bin/zookeeper-server-start.sh -daemon config/zookeeper.properties

echo 'Start Kafka server as daemon'
$KAFKA_HOME/bin/kafka-server-start.sh -daemon config/server.properties
