JC=javac

.PHONY: download extract build-kafka

download: kafka-0.7.2-incubating-src.tgz httpcomponents-client-4.3.3-bin.tar.gz

extract: kafka-0.7.2-incubating-src httpcomponents-client-4.3.3

build-kafka: kafka-0.7.2-incubating-src
	cd kafka-0.7.2-incubating-src && ./sbt update && ./sbt package

kafka-0.7.2-incubating-src.tgz:
	wget http://archive.apache.org/dist/kafka/old_releases/kafka-0.7.2-incubating/kafka-0.7.2-incubating-src.tgz

kafka-0.7.2-incubating-src: kafka-0.7.2-incubating-src.tgz
	tar -xzf kafka-0.7.2-incubating-src.tgz

httpcomponents-client-4.3.3-bin.tar.gz:
	wget http://apache.spinellicreations.com//httpcomponents/httpclient/binary/httpcomponents-client-4.3.3-bin.tar.gz

httpcomponents-client-4.3.3: httpcomponents-client-4.3.3-bin.tar.gz
	tar -xzf httpcomponents-client-4.3.3-bin.tar.gz
