JAVA=java
JC=javac
KAFKAPATH=./kafka-0.7.2-incubating-src
HTTPCOMPONENTSPATH=./httpcomponents-client-4.3.3
CLASSPATH="$(HTTPCOMPONENTSPATH)/lib/commons-codec-1.6.jar:$(HTTPCOMPONENTSPATH)/lib/commons-logging-1.1.3.jar:$(HTTPCOMPONENTSPATH)/lib/fluent-hc-4.3.3.jar:$(HTTPCOMPONENTSPATH)/lib/httpclient-4.3.3.jar:$(HTTPCOMPONENTSPATH)/lib/httpclient-cache-4.3.3.jar:$(HTTPCOMPONENTSPATH)/lib/httpcore-4.3.2.jar:$(HTTPCOMPONENTSPATH)/lib/httpmime-4.3.3.jar:."
KAFKA_OPTS=-Xmx512M -server -Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.port=3333 -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false

.PHONY: download extract build-kafka build clean

download: kafka-0.7.2-incubating-src.tgz httpcomponents-client-4.3.3-bin.tar.gz

extract: kafka-0.7.2-incubating-src httpcomponents-client-4.3.3

build-kafka: kafka-0.7.2-incubating-src kafka-0.7.2-incubating-src/lib_managed

kafka-0.7.2-incubating-src/lib_managed:
	cd kafka-0.7.2-incubating-src && ./sbt update && ./sbt package

build-runner: build-kafka KafkaSpraynozzle.class
	echo '$(JAVA) -cp $(CLASSPATH) KafkaSpraynozzle $$1 $$2' > kafka-spraynozzle.sh
	chmod +x kafka-spraynozzle.sh

build: extract build-kafka build-runner KafkaSpraynozzle.class

KafkaSpraynozzle.class:
	$(JC) -cp $(CLASSPATH) kafka-spraynozzle.java

clean:
	rm -rf kafka-0.7.2-incubating-src
	rm -rf kafka-0.7.2-incubating-src.tgz
	rm -rf httpcomponents-client-4.3.3
	rm -rf httpcomponents-client-4.3.3-bin.tar.gz
	rm -rf KafkaSpraynozzle.class

kafka-0.7.2-incubating-src.tgz:
	wget http://archive.apache.org/dist/kafka/old_releases/kafka-0.7.2-incubating/kafka-0.7.2-incubating-src.tgz

kafka-0.7.2-incubating-src: kafka-0.7.2-incubating-src.tgz
	tar -xzf kafka-0.7.2-incubating-src.tgz

httpcomponents-client-4.3.3-bin.tar.gz:
	wget http://apache.spinellicreations.com//httpcomponents/httpclient/binary/httpcomponents-client-4.3.3-bin.tar.gz

httpcomponents-client-4.3.3: httpcomponents-client-4.3.3-bin.tar.gz
	tar -xzf httpcomponents-client-4.3.3-bin.tar.gz
