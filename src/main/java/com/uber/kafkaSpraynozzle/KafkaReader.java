package com.uber.kafkaSpraynozzle;

import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentLinkedQueue;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import kafka.consumer.KafkaStream;
import kafka.message.Message;
import kafka.message.MessageAndMetadata;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.ContentType;

public class KafkaReader implements Runnable {
    private MetricRegistry metricRegistry;
    private ConcurrentLinkedQueue<ByteArrayEntity> queue;
    private KafkaStream<Message> stream;

    private Counter enqueuedCount;
    private Counter pausedCount;
    private Timer pausedTime;

    public KafkaReader(MetricRegistry metricRegistry, ConcurrentLinkedQueue<ByteArrayEntity> queue, KafkaStream<Message> stream) {
        this.metricRegistry = metricRegistry;
        this.queue = queue;
        this.stream = stream;

        this.enqueuedCount = metricRegistry.counter("enqueued_count");
        this.pausedCount = metricRegistry.counter("pause_count");
        this.pausedTime = metricRegistry.timer("pause_time");
    }

    public void run() {
        long threadId = Thread.currentThread().getId();
        // Supposedly the HTTP Client is threadsafe, but lets not chance it, eh?
        System.out.println("Starting reader thread " + threadId);
        int pushCount = 0;
        for(MessageAndMetadata<Message> msgAndMetadata: this.stream) {
            ByteBuffer message = msgAndMetadata.message().payload();
            Integer messageLen = msgAndMetadata.message().payloadSize();
            Integer messageOffset = message.arrayOffset();
            ByteArrayEntity jsonEntity = new ByteArrayEntity(message.array(), messageOffset, messageLen, ContentType.APPLICATION_JSON);
            jsonEntity.setContentEncoding("UTF-8");
            queue.add(jsonEntity);
            enqueuedCount.inc();
            pushCount++;
            if(pushCount == 100) {
                pushCount = 0;
                int queueSize = queue.size();
                if(queueSize > 500) {
                    pausedCount.inc();
                    try (Timer.Context ctx = pausedTime.time()) {
                        while (queueSize > 100) {
                            try {
                                Thread.sleep(5);
                            } catch (java.lang.InterruptedException e) {
                                System.out.println("Sleep issue!?");
                                e.printStackTrace();
                            }
                            queueSize = queue.size();
                        }
                    }
                }
            }
        }
    }
}
