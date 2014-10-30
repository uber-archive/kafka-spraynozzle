package com.uber.kafkaSpraynozzle;

// Forgive me for any non-idiomatic ways this code behaves. I'm not a Java guy but the other clients suck.
// Also, your build systems are all insane, so I'm not apologizing for the Makefile.
import com.lexicalscope.jewel.cli.ArgumentValidationException;
import com.lexicalscope.jewel.cli.CliFactory;
import java.io.File;
import java.lang.ClassLoader;
import java.lang.ClassNotFoundException;
import java.lang.IllegalAccessException;
import java.lang.InstantiationException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.Message;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;

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
        boolean buffering = spraynozzleArgs.getBuffering();
        final String filterClass = spraynozzleArgs.getFilterClass();
        final String filterClasspath = spraynozzleArgs.getFilterClasspath();
        System.out.println("Listening to " + topic + " topic from " + zk + " and redirecting to " + url);

        if(!buffering) {
            // Clear out zookeeper records so the spraynozzle drops messages between runs
            ZkClient zkClient = new ZkClient(zk, 10000);
            ZkUtils.deletePathRecursive(zkClient, "/consumers/kafka_spraynozzle_" + topic);
            while (ZkUtils.pathExists(zkClient, "/consumers/kafka_spraynozzle_" + topic)) {
                try {
                    Thread.sleep(250);
                } catch (java.lang.InterruptedException e) {
                    System.out.println("Sleep Exception!?");
                    e.printStackTrace();
                }
            }
        }

        KafkaFilter messageFilter = null;
        if (filterClass != null && filterClasspath != null) {
            File file = new File(filterClasspath);
            try {
                URL[] urls = new URL[]{file.toURL()};
                ClassLoader cl = new URLClassLoader(urls);
                Class cls = cl.loadClass(filterClass);
                messageFilter = (KafkaFilter) cls.newInstance();
            } catch (MalformedURLException e) {
                System.out.println("Bad classpath provided");
            } catch (ClassNotFoundException e) {
                System.out.println("Filter class not found");
            } catch (InstantiationException e) {
                System.out.println("Cannot create instance of filter class");
            } catch (IllegalAccessException e) {
                System.out.println("Filter class did I have no idea what but its bad and illegal.");
            }
        }

        if (messageFilter == null) {
            messageFilter = new KafkaNoopFilter();
        }

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
            executor.submit(new KafkaPoster(queue, cm, url, logQueue, messageFilter));
        }
    }
}