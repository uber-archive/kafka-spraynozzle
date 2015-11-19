package com.uber.kafkaSpraynozzle;

import java.util.Date;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.util.EntityUtils;

public class KafkaPoster implements Runnable {
    private static int currentUrl = 0;

    private MetricRegistry metricRegistry;
    private ConcurrentLinkedQueue<ByteArrayEntity> queue;
    private PoolingHttpClientConnectionManager cm;
    private List<String> urls;
    private KafkaFilter messageFilter;
    private RequestConfig requestConfig;

    private Timer postTime;
    private Counter postCount;
    private Counter postSuccess;
    private Counter postFailure;
    private Counter filteredCount;

    public KafkaPoster(
        MetricRegistry metricRegistry,
        ConcurrentLinkedQueue<ByteArrayEntity> queue,
        PoolingHttpClientConnectionManager cm,
        List<String> urls,
        KafkaFilter messageFilter,
        Integer socketTimeout,
        Integer connectionTimeout
    ) {
        this.metricRegistry = metricRegistry;
        this.queue = queue;
        this.cm = cm;
        this.urls = urls;
        this.messageFilter = messageFilter;
        RequestConfig.Builder builder = RequestConfig.custom();
        if (socketTimeout != null) {
            builder = builder.setSocketTimeout(socketTimeout);
        }
        if (connectionTimeout != null) {
            builder = builder.setConnectTimeout(connectionTimeout);
        }
        this.requestConfig = builder.build();

        this.postTime = metricRegistry.timer("post_time");
        this.postCount = metricRegistry.counter("post_count");
        this.postSuccess = metricRegistry.counter("post_success");
        this.postFailure = metricRegistry.counter("post_failure");
        this.filteredCount = metricRegistry.counter("filtered_count");
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

                    try (Timer.Context ctx = postTime.time()) {
                        postCount.inc();
                        HttpPost post = new HttpPost(urls.get(currentUrl));
                        currentUrl = (currentUrl + 1) % urls.size();
                        post.setHeader("User-Agent", "KafkaSpraynozzle-0.0.1");
                        post.setEntity(jsonEntity);
                        post.setConfig(requestConfig);
                        CloseableHttpResponse response = client.execute(post);
                        int statusCode = response.getStatusLine().getStatusCode();
                        if (statusCode >= 200 && statusCode < 300) {
                            postSuccess.inc();
                        } else {
                            postFailure.inc();
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
                        postFailure.inc();
                    }
                } else {
                    filteredCount.inc();
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
