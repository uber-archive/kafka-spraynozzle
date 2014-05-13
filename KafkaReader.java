import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentLinkedQueue;
import kafka.consumer.KafkaStream;
import kafka.message.Message;
import kafka.message.MessageAndMetadata;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.ContentType;

public class KafkaReader implements Runnable {
    ConcurrentLinkedQueue<ByteArrayEntity> queue;
    KafkaStream<Message> stream;

    public KafkaReader(ConcurrentLinkedQueue<ByteArrayEntity> queue, KafkaStream<Message> stream) {
        this.queue = queue;
        this.stream = stream;
    }

    public void run() {
        long threadId = Thread.currentThread().getId();
        // Supposedly the HTTP Client is threadsafe, but lets not chance it, eh?
        System.out.println("Starting thread " + threadId);
        int pushCount = 0;
        for(MessageAndMetadata msgAndMetadata: this.stream) {
            // There's no retry logic or anything like that, so the least I can do
            // is log about incoming messages and the status code I get back from the server.
            // Be sure to redirect stdout and stderr to files or your perf will tank.
            System.out.println("Processing message");
            // Why the heck do I need to cast the message object back into a Message type?
            // `msgAndOffset` doesn't have this wart. :(
            ByteBuffer message = ((Message)msgAndMetadata.message()).payload();
            Integer messageLen = ((Message)msgAndMetadata.message()).payloadSize();
            Integer messageOffset = message.arrayOffset();
            ByteArrayEntity jsonEntity = new ByteArrayEntity(message.array(), messageOffset, messageLen, ContentType.APPLICATION_JSON);
            jsonEntity.setContentEncoding("UTF-8");
            queue.add(jsonEntity);
            System.out.println("Enqueued for posting");
            pushCount++;
            if(pushCount == 100) {
                pushCount = 0;
                int queueSize = queue.size();
                if(queueSize > 100) {
                    while(queueSize > 10) {
                        System.out.println("Nozzle is clogged. Sleeping to clear out");
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
