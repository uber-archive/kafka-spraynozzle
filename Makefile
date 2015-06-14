JAVA=java
JC=javac
JAR=jar
KAFKAPATH=./kafka-0.7.2-incubating-src
HTTPCOMPONENTSPATH=./httpcomponents-client-4.3.6
CLASSPATH="$(HTTPCOMPONENTSPATH)/lib/commons-codec-1.6.jar:$(HTTPCOMPONENTSPATH)/lib/commons-logging-1.1.3.jar:$(HTTPCOMPONENTSPATH)/lib/fluent-hc-4.3.6.jar:$(HTTPCOMPONENTSPATH)/lib/httpclient-4.3.6.jar:$(HTTPCOMPONENTSPATH)/lib/httpclient-cache-4.3.6.jar:$(HTTPCOMPONENTSPATH)/lib/httpcore-4.3.3.jar:$(HTTPCOMPONENTSPATH)/lib/httpmime-4.3.6.jar:$(KAFKAPATH)/project/boot/scala-2.8.0/lib/scala-compiler.jar:$(KAFKAPATH)/project/boot/scala-2.8.0/lib/scala-library.jar:$(KAFKAPATH)/core/target/scala_2.8.0/kafka-0.7.2.jar:$(KAFKAPATH)/core/lib/*.jar:$(KAFKAPATH)/perf/target/scala_2.8.0/kafka-perf-0.7.2.jar:$(KAFKAPATH)/core/lib_managed/scala_2.8.0/compile/jopt-simple-3.2.jar:$(KAFKAPATH)/core/lib_managed/scala_2.8.0/compile/log4j-1.2.15.jar:$(KAFKAPATH)/core/lib_managed/scala_2.8.0/compile/snappy-java-1.0.4.1.jar:$(KAFKAPATH)/core/lib_managed/scala_2.8.0/compile/zkclient-0.1.jar:$(KAFKAPATH)/core/lib_managed/scala_2.8.0/compile/zookeeper-3.3.4.jar:./jewelcli-0.8.7.jar:./kafkaSpraynozzle.jar"
KAFKA_OPTS=-Xmx512M -server -Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.port=3333 -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false

.PHONY: download extract build-kafka build rebuild clean

download: kafka-0.7.2-incubating-src.tgz httpcomponents-client-4.3.6-bin.tar.gz jewelcli-0.8.7.jar

extract: kafka-0.7.2-incubating-src httpcomponents-client-4.3.6

build-kafka: kafka-0.7.2-incubating-src kafka-0.7.2-incubating-src/lib_managed jewelcli-0.8.7.jar

kafka-0.7.2-incubating-src/lib_managed:
	cd kafka-0.7.2-incubating-src && ./sbt update && ./sbt package

build-runner: build-kafka
#KafkaSpraynozzle.class
	echo 'exec -a kafka-spraynozzle-$${@: -1} $(JAVA) -cp $(CLASSPATH) com.uber.kafkaSpraynozzle.KafkaSpraynozzle "$$@"' > kafka-spraynozzle.sh
	chmod +x kafka-spraynozzle.sh

build: extract build-kafka build-runner
	$(JC) -Xlint:unchecked -cp $(CLASSPATH) com/uber/kafkaSpraynozzle/*.java com/uber/kafkaSpraynozzle/*/*.java
	$(JAR) cfe kafkaSpraynozzle.jar com.uber.kafkaSpraynozzle.KafkaSpraynozzle com/uber/kafkaSpraynozzle/*.class com/uber/kafkaSpraynozzle/*/*.class

rebuild: extract
	rm *.class || exit 0
	make build

clean:
	rm -rf kafka-0.7.2-incubating-src
	rm -rf kafka-0.7.2-incubating-src.tgz
	rm -rf httpcomponents-client-4.3.6
	rm -rf httpcomponents-client-4.3.6-bin.tar.gz
	rm -rf com/uber/kafkaSpraynozzle/*.class
	rm -rf com/uber/kafkaSpraynozzle/*/*.class
	rm -rf *.class
	rm -rf *.jar

kafka-0.7.2-incubating-src.tgz:
	wget http://archive.apache.org/dist/kafka/old_releases/kafka-0.7.2-incubating/kafka-0.7.2-incubating-src.tgz

kafka-0.7.2-incubating-src: kafka-0.7.2-incubating-src.tgz
	tar -xzf kafka-0.7.2-incubating-src.tgz

httpcomponents-client-4.3.6-bin.tar.gz:
	wget http://apache.spinellicreations.com//httpcomponents/httpclient/binary/httpcomponents-client-4.3.6-bin.tar.gz

httpcomponents-client-4.3.6: httpcomponents-client-4.3.6-bin.tar.gz
	tar -xzf httpcomponents-client-4.3.6-bin.tar.gz

jewelcli-0.8.7.jar:
	wget http://repo1.maven.org/maven2/com/lexicalscope/jewelcli/jewelcli/0.8.7/jewelcli-0.8.7.jar
