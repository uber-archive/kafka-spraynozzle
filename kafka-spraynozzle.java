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
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.consumer.KafkaStream;
import kafka.message.Message;
import kafka.message.MessageAndMetadata;
import kafka.utils.Utils;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.util.EntityUtils;

class KafkaSpraynozzle {
    // Indentation good!
    public static void main(String[] args) {
        // TODO: Make the threadCount an argument as soon as I write a good topic tester
        // to determine the best threadCount for a given topic.
        final Integer threadCount = Integer.parseInt(args[3]);
        String topic = args[0];
        final String url = args[1];
        String zk = args[2];
        System.out.println("Listening to " + topic + " topic from " + zk + " and redirecting to " + url + " (not really)");

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
        topicParallelism.put(topic, threadCount);
        Map<String, List<KafkaStream<Message>>> topicMessageStreams = consumerConnector.createMessageStreams(topicParallelism);
        List<KafkaStream<Message>> streams = topicMessageStreams.get(topic);
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);

        // Http Connection Pooling stuff
        final PoolingHttpClientConnectionManager cm = new PoolingHttpClientConnectionManager();
        cm.setMaxTotal(200);
        cm.setDefaultMaxPerRoute(20);

        // I'll admit this is pretty cool, but the syntax for `Runnable`s is weird
        for(final KafkaStream<Message> stream: streams) {
            executor.submit(new Runnable() {
                public void run() {
                    // Supposedly the HTTP Client is threadsafe, but lets not chance it, eh?
                    System.out.println("Starting thread");
                    CloseableHttpClient client = HttpClientBuilder.create().setConnectionManager(cm).build();
                    for(MessageAndMetadata msgAndMetadata: stream) {
                        // There's no retry logic or anything like that, so the least I can do
                        // is log about incoming messages and the status code I get back from the server.
                        // Be sure to redirect stdout and stderr to files or your perf will tank.
                        System.out.println("Processing message");
                        HttpPost post = new HttpPost(url);
                        post.setHeader("User-Agent", "KafkaSpraynozzle-0.0.1");
                        try {
                            // Why the heck do I need to cast the message object back into a Message type?
                            // `msgAndOffset` doesn't have this wart. :(
                            ByteBuffer message = ((Message)msgAndMetadata.message()).payload();
                            Integer messageLen = ((Message)msgAndMetadata.message()).payloadSize();
                            Integer messageOffset = message.arrayOffset();
                            ByteArrayEntity jsonEntity = new ByteArrayEntity(message.array(), messageOffset, messageLen, ContentType.APPLICATION_JSON);
                            jsonEntity.setContentEncoding("UTF-8");
                            post.setEntity(jsonEntity);
                            // The fact that `CloseableHttpClient` and `CloseableHttpResponse` are separate types
                            // confuses me, and led to one of the hardest to track bugs in earlier version of this code.
                            CloseableHttpResponse response = client.execute(post);
                            System.out.println("Response code: " + response.getStatusLine().getStatusCode());
                            EntityUtils.consume(response.getEntity());
                        } catch (java.io.UnsupportedEncodingException e) {
                            // I love that the compiler made me catch these exceptions.
                            // At least for now, though, retries are not in scope, so just be loud when this happens.
                            System.out.println("Encoding issue");
                            e.printStackTrace();
                        } catch (java.io.IOException e) {
                            System.out.println("IO issue");
                            e.printStackTrace();
                        }
                    }
                }
            });
        }
    }
}
