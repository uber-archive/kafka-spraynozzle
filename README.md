# kafka-spraynozzle

A nozzle to spray a kafka topic at an HTTP endpoint.

## Why?

Intended to be used in combination with a load-balancing service such as HAProxy, NginX, or one built into a language like Node Cluster.

Most non-Java Kafka clients suck, especially the Kafka 0.7 clients, so they can't handle the load of a truly high throughput kafka stream. This project trades the reachability guarantees of Kafka for a protocol that essentially every language out there has an optimized codebase for: HTTP.

*DO NOT* use this if you expect any of the following from Kafka:

* Messages are always delivered to the client because the client asks for the data upfront.
* Messages are not lost during a service restart.
* Older messages can be re-queried for if later analysis deems them important for a second, more detailed pass.
* You can't scale your service horizontally any further and need a vertical solution.

*DO* use this if any of the following about Kafka has impacted you:

* Your Kafka topics have become so high throughput that you need multipler workers, so you first have your master process just consume and feed data to child workers.
* Then that's not enough, so you throw away the Kafka aggregate concept and couple your consumer's internals to your producer's internals and feed directly on the producer servers.
* Then that's not enough, so you couple the producers and consumers more by breaking the producer's topic into many topics. Maybe you reintroduce the aggregate, maybe you don't.
* You huddle in fear when the most logically segmented kafka topics become too burdensome for the consumer to bear.

## Usage

Make sure you have a relatively up-to-date Java installed. I'm not really a Java guy, so not sure what the minimum supported version would be, but I used `javac 1.6.0_27`.

Also make sure you have `make` available. It's used to build the project. Optionally have any version of Node.js installed if you want to run the trivial test server that was used in debugging the spraynozzle.

Finally, you can do:

    git clone git@github.com:uber/kafka-spraynozzle
    cd kafka-spraynozzle
    make build
    ./kafka-spraynozzle.sh kafka.topic http://remote.server:port zookeeper.server:port

## License (MIT)

Copyright (C) 2014 by Uber Technologies, Inc

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.