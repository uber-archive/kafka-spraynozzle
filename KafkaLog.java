import java.util.concurrent.ConcurrentLinkedQueue;

// Stringly-typed for now. Anti-pattern but will make it grown up when this app demands more rigor on this front.
// *Highly* coupled with the other classes, but to move beyond that will need some crazy mechanism for generating
// an aggregrated log line about arbitrary events that still looks somewhat like english and why bother with that now?
public class KafkaLog implements Runnable {
    ConcurrentLinkedQueue<String> logQueue;
    String topic;
    String url;

    public KafkaLog(ConcurrentLinkedQueue<String> logQueue, String topic, String url) {
        this.logQueue = logQueue;
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
            while((log = this.logQueue.poll()) != null) {
                if(log == "enqueued") {
                    enqueued++;
                } else if(log == "clogged") {
                    clogged++;
                } else if(log == "posting") {
                    posting++;
                } else if(log == "postSuccess") {
                    postSuccess++;
                } else if(log == "postFailure") {
                    postFailure++;
                }
            }
            System.out.println("kafka-spraynozzle grabbed " + enqueued + " messages from " + this.topic + ", posted " + posting + " messages to " + this.url + " with " + postSuccess + " succeeding and " + postFailure + " failing");
        }
    }
}
