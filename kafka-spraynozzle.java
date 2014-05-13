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
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.Date;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.consumer.KafkaStream;
import kafka.message.Message;
import kafka.message.MessageAndMetadata;
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
        ZkUtils.deletePathRecursive(zkClient, "/consumers/kafka_spraynozzle_" + topic);

        // Kafka setup stuff
        Properties kafkaProps = new Properties();
        kafkaProps.put("zk.connect", zk);
        kafkaProps.put("zk.connectiontimeout.ms", "10000");
        kafkaProps.put("groupid", "kafka_spraynozzle_" + topic);
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

        // Build the worker threads
        for(final KafkaStream<Message> stream: streams) {
            executor.submit(new KafkaReader(queue, stream));
        }

        for(int i = 0; i < threadCount; i++) {
            executor.submit(new KafkaPoster(queue, cm, url));
        }
    }
}
