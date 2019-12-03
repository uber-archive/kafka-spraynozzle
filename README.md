# kafka-spraynozzle

(This project is deprecated and not maintained.)

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

Also make sure you have `maven` installed. It's used to build the project. Optionally have any version of Node.js installed if you want to run the trivial test server that was used in debugging the spraynozzle.

Finally, you can do:

```sh
git clone git@github.com:uber/kafka-spraynozzle.git
cd kafka-spraynozzle
mvn clean package
./kafka-spraynozzle.sh -n num_http_threads -u http://remote.server:port/url -z zookeeper.server:port kafka.topic
```

## How It Works

Threads! Wonderful, magical threads!

![THREADS](https://docs.google.com/a/uber.com/drawings/d/1SjEXmgpvUOQGE9HMfXf_i01EykhQXC0Nhg5RaHQeVts/pub?w=1348&h=590)

More seriously, the spraynozzle has two types of worker threads: kafka partition consumer threads and http posting threads, with a thread-safe queue created on the main thread gluing them together. The consumer threads read from their designated partition as fast as possible and unwrap the message from kafka's format and rewrap it into a format the http clients can understand, then push it onto the queue. The posting threads read from the top of the queue, construct a post request, stuff in the message, and then read and immediately throw away the response (to keep the http socket alive and not consume all available sockets on the box).

This aggregation and re-dissemination seems silly when you stop to think about it, but what it's actually doing is working around a design flaw in Kafka: the producer has to know the maximum amount of data it can throw into a single partition based on however much any individual consuming worker can handle. This is a design flaw because:

1. You can't predict that ahead of time.
2. Even if you do figure it out for one consumer, it'll be different for another even if its written in the same language because the amount of work it does on the data differs.

So kafka-spraynozzle actually has *three* knobs to adjust involving data flow:

1. The number of partitions to consume. That's easy, just set it to the partition count of the topic at hand.
2. The number of http threads to run. This one depends on the response time of your workers and the messages/sec of your topic. 1/response time gives you the messages/sec of each http worker, so messages/sec/response time gives you the number of http threads to run.
3. The number of workers sitting behind HAProxy or NginX or Node Cluster or whatever. This is also relatively simple: the number of messages/sec of your topic divided by the number of messages/sec each worker can *safely* consume (there should be some buffer for spikes in the stream to not choke your workers).

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
