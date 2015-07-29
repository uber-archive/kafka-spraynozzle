exec -a kafka-spraynozzle-${@: -1} java -classpath ./target/kafkaSpraynozzle-0.1.0.jar com.uber.kafkaSpraynozzle.KafkaSpraynozzle "$@"
