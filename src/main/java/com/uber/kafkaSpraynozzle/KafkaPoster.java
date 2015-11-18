package com.uber.kafkaSpraynozzle;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import org.apache.http.client.config.RequestConfig;
import kafka.common.InvalidMessageSizeException;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.util.EntityUtils;

public class KafkaPoster implements Runnable {
    private MetricRegistry metricRegistry;
    ConcurrentLinkedQueue<ByteArrayEntity> queue;
    PoolingHttpClientConnectionManager cm;
    List<String> urls;
    int currentUrl = 0;
    static final int responseDecayingHalflife = 5;
    KafkaFilter messageFilter;
    private RequestConfig requestConfig;
    private boolean roundRobin = false;
    private int batchSize = 20;
    private int batchMaxBytes = 128*1024;
    private boolean debuggingOutput = false;
    private Timer postTime = new Timer();
    private Counter postCount = new Counter();
    private Counter postSuccess = new Counter();
    private Counter postFailure = new Counter();
    private Counter filteredCount = new Counter();

    public KafkaPoster(
            MetricRegistry metricRegistry,
            ConcurrentLinkedQueue<ByteArrayEntity> queue,
            PoolingHttpClientConnectionManager cm,
            List<String> urls,
            KafkaFilter messageFilter,
            Integer socketTimeout,
            Integer connectionTimeout
            ){
        this(metricRegistry,
                queue, cm, urls, messageFilter,
                socketTimeout, connectionTimeout,
                20, false, false );
    }
    public KafkaPoster(
        MetricRegistry metricRegistry,
        ConcurrentLinkedQueue<ByteArrayEntity> queue,
        PoolingHttpClientConnectionManager cm,
        List<String> urls,
        KafkaFilter messageFilter,
        Integer socketTimeout,
        Integer connectionTimeout,
        int batchSize,
        boolean roundRobinPost,
        boolean debuggingOutput) {
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

        if (metricRegistry != null) {
            this.postTime = metricRegistry.timer("post_time");
            this.postCount = metricRegistry.counter("post_count");
            this.postSuccess = metricRegistry.counter("post_success");
            this.postFailure = metricRegistry.counter("post_failure");
            this.filteredCount = metricRegistry.counter("filtered_count");
        }
        this.batchSize = batchSize;
        this.roundRobin = roundRobinPost;
        this.debuggingOutput = debuggingOutput;
    }

    private int leastResponseIdx(long[] responseTime) {
        int leastIdx = 0;
        for (int i = 1; i < responseTime.length; ++i){
            if (responseTime[i] < responseTime[leastIdx]){
                leastIdx = i;
            }
        }
        return leastIdx;
    }

    private int getBatchDataSize(List<ByteArrayEntity> batch){
        int totalSize = 0;
        for (int i = 0; i < batch.size(); ++i){
            totalSize += (int)batch.get(i).getContentLength();
        }
        return totalSize;
    }

    private CloseableHttpClient client = null;
    private long threadId = 0L;
    private long lastReconnect = 0L;
    private long[] responseTime = null;
    private long[] responseTimestamp = null;
    public void run() {
        threadId = Thread.currentThread().getId();
        System.out.println("Starting poster thread " + threadId);
        client = HttpClientBuilder.create().setConnectionManager(cm).build();
        lastReconnect = new Date().getTime();
        responseTime = new long[urls.size()];
        responseTimestamp = new long[urls.size()];

        List<ByteArrayEntity> batch = new ArrayList<ByteArrayEntity>(batchSize + 1);

        long totalPostingTimeMs = 0L;
        long totalPostingCount = 0L;
        long lastReportingTime = new Date().getTime();
        while(true) {
            ByteArrayEntity jsonEntity = queue.poll();
            if(jsonEntity != null) {
                jsonEntity = messageFilter.filter(jsonEntity);
                if (jsonEntity != null) {
                    batch.add(jsonEntity);

                    if (batch.size() >= batchSize || getBatchDataSize(batch) >= batchMaxBytes) {
                        long timeBeforePost = new Date().getTime();
                        if (debuggingOutput && timeBeforePost - lastReportingTime > 5*1000) {
                            System.out.println(String.format(
                                    "Posting thread %d, avg posting cost %d ms, total posting called %d",
                                    threadId, totalPostingTimeMs / totalPostingCount, totalPostingCount));
                            lastReportingTime = timeBeforePost;
                        }
                        postBatchEvents(batch);
                        totalPostingCount += 1;
                        totalPostingTimeMs += (new Date().getTime() - timeBeforePost);
                    }
                } else {
                    filteredCount.inc();
                }
            } else {
                postBatchEvents(batch);
                try {
                    Thread.sleep(250);
                } catch (java.lang.InterruptedException e) {
                    System.out.println("Sleep issue!?");
                    e.printStackTrace();
                }
            }
        }
    }

    private boolean postBatchEvents(List<ByteArrayEntity> batch) {
        final long NANOS_PER_SECOND = 1000L * 1000L * 1000L;
        final long NANOS_PER_MILLI_SECOND = 1000L * 1000L;
        int pickedUrlIdx;
        try (Timer.Context ctx = postTime.time()) {
            long timeBeforePost = new Date().getTime();
            postCount.inc();
            if (roundRobin) {
                pickedUrlIdx = currentUrl;
            } else {
                pickedUrlIdx = leastResponseIdx(responseTime);
            }
            HttpPost post = new HttpPost(urls.get(pickedUrlIdx));
            if (roundRobin) {
                currentUrl = (currentUrl + 1) % urls.size();
            }
            post.setHeader("User-Agent", "KafkaSpraynozzle-0.2.0");
            if (batchSize == 1 && batch.size() == 1){
                post.setEntity(batch.get(0));
                batch.clear();
            }else {
                try {
                    ByteArrayEntity packedEntities = ListSerializer.toByteArrayEntity(batch);
                    post.setEntity(packedEntities);
                    batch.clear();
                } catch (InvalidMessageSizeException ex) {
                    postFailure.inc();
                    batch.clear();
                    return false;
                }
            }
            CloseableHttpResponse response = client.execute(post);
            int statusCode = response.getStatusLine().getStatusCode();
            long timeAfterPost = System.nanoTime();
            if (statusCode >= 200 && statusCode < 300) {
                postSuccess.inc();
            } else {
                postFailure.inc();
            }


            //// managing the "least response time" decay.
            //// Every time halflife time has passed, we reduce the "bad record" of response time by half
            /// so eventually those bad servers with slow response time will get another chance of being tried.
            /// and we will not keep pounding the same fast server since all record of response time decays.
            responseTimestamp[pickedUrlIdx] = timeAfterPost;
            responseTime[pickedUrlIdx] = (timeAfterPost - timeBeforePost) + 2 * NANOS_PER_MILLI_SECOND;// penalize by 2 ms so the same won't be used again and again.
            for (int i = 0; i < responseTimestamp.length; ++i) {
                if ((timeAfterPost - responseTimestamp[i]) > responseDecayingHalflife * NANOS_PER_SECOND) {
                    responseTimestamp[i] = timeAfterPost;
                    responseTime[i] = responseTime[i] / 2;
                }
            }
            long currentTime = new Date().getTime();
            if (currentTime - lastReconnect > 10000) {
                lastReconnect = currentTime;
                response.close();
            } else {
                EntityUtils.consume(response.getEntity());
            }
        } catch (IOException e) {
            System.out.println("IO issue");
            e.printStackTrace();
            postFailure.inc();
            return false;
        }
        return true;
    }
}
