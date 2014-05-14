// Forgive me for any non-idiomatic ways this code behaves. I'm not a Java guy but the other clients suck.
// Also, your build systems are all insane, so I'm not apologizing for the Makefile.
import java.util.Properties;
import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ConcurrentLinkedQueue;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.consumer.KafkaStream;
import kafka.message.Message;
import kafka.utils.ZkUtils;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.I0Itec.zkclient.ZkClient;
import com.lexicalscope.jewel.cli.CliFactory;
import com.lexicalscope.jewel.cli.ArgumentValidationException;

class KafkaSpraynozzle {
    // Indentation good!
    public static void main(String[] args) {
        KafkaArgs spraynozzleArgs;
        try {
            spraynozzleArgs = CliFactory.parseArguments(KafkaArgs.class, args);
        } catch(ArgumentValidationException e) {
            System.err.println(e.getMessage());
            return;
        }
        String topic = spraynozzleArgs.getTopic();
        final String url = spraynozzleArgs.getUrl();
        String zk = spraynozzleArgs.getZk();
        final int threadCount = spraynozzleArgs.getThreadCount();
        final int partitionCount = spraynozzleArgs.getPartitionCount();
        System.out.println("Listening to " + topic + " topic from " + zk + " and redirecting to " + url);

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
        ExecutorService executor = Executors.newFixedThreadPool(threadCount+partitionCount+1);

        // Http Connection Pooling stuff
        final PoolingHttpClientConnectionManager cm = new PoolingHttpClientConnectionManager();
        cm.setMaxTotal(threadCount);
        cm.setDefaultMaxPerRoute(threadCount);

        // Message-passing queues within the spraynozzle
        final ConcurrentLinkedQueue<ByteArrayEntity> queue = new ConcurrentLinkedQueue<ByteArrayEntity>();
        final ConcurrentLinkedQueue<String> logQueue = new ConcurrentLinkedQueue<String>();

        // Build the worker threads
        executor.submit(new KafkaLog(logQueue, topic, url));

        for(final KafkaStream<Message> stream: streams) {
            executor.submit(new KafkaReader(queue, stream, logQueue));
        }

        for(int i = 0; i < threadCount; i++) {
            executor.submit(new KafkaPoster(queue, cm, url, logQueue));
        }
    }
}
