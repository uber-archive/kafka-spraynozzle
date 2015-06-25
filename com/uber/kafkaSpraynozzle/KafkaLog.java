package com.uber.kafkaSpraynozzle;

import com.uber.kafkaSpraynozzle.stats.StatsReporter;

import java.util.concurrent.ConcurrentLinkedQueue;

// Stringly-typed for now. Anti-pattern but will make it grown up when this app demands more rigor on this front.
// *Highly* coupled with the other classes, but to move beyond that will need some crazy mechanism for generating
// an aggregrated log line about arbitrary events that still looks somewhat like english and why bother with that now?
public class KafkaLog implements Runnable {
    private static final String LOG_FORMAT =
        "{" +
            "\"message\": \"kafka-spraynozzle stats\", " +
            "\"data\": {" +
                "\"topic\": \"%s\", " +
                "\"enqueued_count\": %d, " +
                "\"filtered_count\": %d, " +
                "\"pause_count\": %d, " +
                "\"post_url\": \"%s\", " +
                "\"post_count\": %d, " +
                "\"post_success\": %d, " +
                "\"post_failure\": %d" +
            "}" +
        "}";

    ConcurrentLinkedQueue<String> logQueue;
    StatsReporter statsReporter;
    String topic;
    String url;

    public KafkaLog(ConcurrentLinkedQueue<String> logQueue, StatsReporter statsReporter, String topic, String url) {
        this.logQueue = logQueue;
        this.statsReporter = statsReporter;
        this.topic = topic;
        this.url = url;
    }

    public void run() {
        long threadId = Thread.currentThread().getId();
        System.out.println("Starting logger thread " + threadId);
        while(true) {
            try {
                Thread.sleep(10000);
            } catch (java.lang.InterruptedException e) {
                System.out.println("Sleep issue!?");
                e.printStackTrace();
            }
            String log;
            int enqueued = 0;
            int clogged = 0;
            int posting = 0;
            int postSuccess = 0;
            int postFailure = 0;
            int filteredOut = 0;
            while((log = this.logQueue.poll()) != null) {
                if("enqueued".equals(log)) {
                    enqueued++;
                } else if("clogged".equals(log)) {
                    clogged++;
                } else if("posting".equals(log)) {
                    posting++;
                } else if("postSuccess".equals(log)) {
                    postSuccess++;
                } else if("postFailure".equals(log)) {
                    postFailure++;
                } else if("filteredOut".equals(log)) {
                    filteredOut++;
                }
            }
            System.out.println(String.format(LOG_FORMAT, this.topic, enqueued, filteredOut, clogged, this.url, posting, postSuccess, postFailure));
            statsReporter.count("enqueued", enqueued);
            statsReporter.count("paused", clogged);
            statsReporter.count("posted", posting);
            statsReporter.count("postSuccess", postSuccess);
            statsReporter.count("postFailure", postFailure);
        }
    }
}