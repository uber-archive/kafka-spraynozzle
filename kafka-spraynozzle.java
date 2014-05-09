// Forgive me for any non-idiomatic ways this code behaves. I'm not a Java guy but the other clients suck.
// Also, your build systems are all insane, so I'm not apologizing for the Makefile.
import java.util.Properties;
import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.lang.Runnable;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.ConcurrentLinkedQueue;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.consumer.KafkaStream;
import kafka.message.Message;
import kafka.message.MessageAndMetadata;
import kafka.utils.Utils;
import kafka.utils.ZkUtils;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.util.EntityUtils;
import org.I0Itec.zkclient.ZkClient;

class KafkaSpraynozzle {
    // Indentation good!
    public static void main(String[] args) {
        String topic = args[0];
        final String url = args[1];
        String zk = args[2];
        final Integer threadCount = Integer.parseInt(args[3]);
        final Integer partitionCount = Integer.parseInt(args[4]);
        System.out.println("Listening to " + topic + " topic from " + zk + " and redirecting to " + url + " (not really)");

        // Clear out zookeeper records so the spraynozzle drops messages between runs
        ZkClient zkClient = new ZkClient(zk, 10000);
        ZkUtils.deletePath(zkClient, "/consumers/kafka_spraynozzle");

        // Kafka setup stuff
        Properties kafkaProps = new Properties();
        kafkaProps.put("zk.connect", zk);
        kafkaProps.put("zk.connectiontimeout.ms", "10000");
        kafkaProps.put("groupid", "kafka_spraynozzle");
        kafkaProps.put("autooffset.reset", "largest");
        kafkaProps.put("fetch.size", String.valueOf(2*1024*1024));
        ConsumerConfig consumerConfig = new ConsumerConfig(kafkaProps);
        ConsumerConnector consumerConnector = Consumer.createJavaConsumerConnector(consumerConfig);
        HashMap<String, Integer> topicParallelism = new HashMap<String, Integer>();
        topicParallelism.put(topic, partitionCount);
        Map<String, List<KafkaStream<Message>>> topicMessageStreams = consumerConnector.createMessageStreams(topicParallelism);
        List<KafkaStream<Message>> streams = topicMessageStreams.get(topic);
        ExecutorService executor = Executors.newFixedThreadPool(threadCount+partitionCount);

        // Http Connection Pooling stuff
        final PoolingHttpClientConnectionManager cm = new PoolingHttpClientConnectionManager();
        cm.setMaxTotal(threadCount);
        cm.setDefaultMaxPerRoute(threadCount);

        final ConcurrentLinkedQueue<ByteArrayEntity> queue = new ConcurrentLinkedQueue<ByteArrayEntity>();

        // I'll admit this is pretty cool, but the syntax for `Runnable`s is weird
        for(final KafkaStream<Message> stream: streams) {
            executor.submit(new Runnable() {
                public void run() {
                    long threadId = Thread.currentThread().getId();
                    // Supposedly the HTTP Client is threadsafe, but lets not chance it, eh?
                    System.out.println("Starting thread " + threadId);
                    int pushCount = 0;
                    for(MessageAndMetadata msgAndMetadata: stream) {
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
            });
        }

        for(int i = 0; i < threadCount; i++) {
            executor.submit(new Runnable() {
                public void run() {
                    long threadId = Thread.currentThread().getId();
                    System.out.println("Starting thread " + threadId);
                    CloseableHttpClient client = HttpClientBuilder.create().setConnectionManager(cm).build();
                    while(true) {
                        ByteArrayEntity jsonEntity = queue.poll();
                        if(jsonEntity != null) {
                            try {
                                System.out.println("Posting message");
                                HttpPost post = new HttpPost(url);
                                post.setHeader("User-Agent", "KafkaSpraynozzle-0.0.1");
                                post.setEntity(jsonEntity);
                                CloseableHttpResponse response = client.execute(post);
                                System.out.println("Response code: " + response.getStatusLine().getStatusCode());
                                EntityUtils.consume(response.getEntity());
                            } catch (java.io.IOException e) {
                                System.out.println("IO issue");
                                e.printStackTrace();
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
            });
        }
    }
}
