package com.uber.kafkaSpraynozzle;

import java.util.Date;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.util.EntityUtils;

public class KafkaPoster implements Runnable {
    ConcurrentLinkedQueue<ByteArrayEntity> queue;
    PoolingHttpClientConnectionManager cm;
    List<String> urls;
    static int currentUrl = 0;
    ConcurrentLinkedQueue<String> logQueue;
    KafkaFilter messageFilter;

    public KafkaPoster(
        ConcurrentLinkedQueue<ByteArrayEntity> queue,
        PoolingHttpClientConnectionManager cm,
        List<String> urls,
        ConcurrentLinkedQueue<String> logQueue,
        KafkaFilter messageFilter) {
        this.queue = queue;
        this.cm = cm;
        this.urls = urls;
        this.logQueue = logQueue;
        this.messageFilter = messageFilter;
    }

    public void run() {
        long threadId = Thread.currentThread().getId();
        System.out.println("Starting poster thread " + threadId);
        CloseableHttpClient client = HttpClientBuilder.create().setConnectionManager(cm).build();
        long lastReconnect = new Date().getTime();
        while(true) {
            ByteArrayEntity jsonEntity = queue.poll();
            if(jsonEntity != null) {
                jsonEntity = messageFilter.filter(jsonEntity);
                if (jsonEntity != null) {
                    try {
                        logQueue.add("posting");
                        HttpPost post = new HttpPost(urls.get(currentUrl));
                        currentUrl = (currentUrl + 1) % urls.size();
                        post.setHeader("User-Agent", "KafkaSpraynozzle-0.0.1");
                        post.setEntity(jsonEntity);
                        CloseableHttpResponse response = client.execute(post);
                        int statusCode = response.getStatusLine().getStatusCode();
                        if (statusCode >= 200 && statusCode < 300) {
                            logQueue.add("postSuccess");
                        } else {
                            logQueue.add("postFailure");
                        }
                        long currentTime = new Date().getTime();
                        if(currentTime - lastReconnect > 10000) {
                            lastReconnect = currentTime;
                            response.close();
                        } else {
                            EntityUtils.consume(response.getEntity());
                        }
                    } catch (java.io.IOException e) {
                        System.out.println("IO issue");
                        e.printStackTrace();
                    }
                } else {
                    logQueue.add("filteredOut");
                }
            } else {
                try {
                    Thread.sleep(250);
                } catch (java.lang.InterruptedException e) {
                    System.out.println("Sleep issue!?");
                    e.printStackTrace();
                }
            }
        }
    }
}
