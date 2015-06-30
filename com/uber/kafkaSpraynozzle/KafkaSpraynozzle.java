package com.uber.kafkaSpraynozzle;

// Forgive me for any non-idiomatic ways this code behaves. I'm not a Java guy but the other clients suck.
// Also, your build systems are all insane, so I'm not apologizing for the Makefile.
import com.lexicalscope.jewel.cli.ArgumentValidationException;
import com.lexicalscope.jewel.cli.CliFactory;
import com.uber.kafkaSpraynozzle.stats.NoopStatsReporter;
import com.uber.kafkaSpraynozzle.stats.StatsReporter;
import java.io.File;
import java.lang.ClassLoader;
import java.lang.ClassNotFoundException;
import java.lang.IllegalAccessException;
import java.lang.InstantiationException;
import java.lang.reflect.InvocationTargetException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
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
        String cleanedUrl = url.replaceAll("[/\\:]", "_");
        String zk = spraynozzleArgs.getZk();
        final int threadCount = spraynozzleArgs.getThreadCount();
        final int partitionCount = spraynozzleArgs.getPartitionCount();
        boolean buffering = spraynozzleArgs.getBuffering();
        final String filterClass = spraynozzleArgs.getFilterClass();
        final String filterClasspath = spraynozzleArgs.getFilterClasspath();
        final String filterClassArgs = spraynozzleArgs.getFilterClassArgs();
        final String statsClass = spraynozzleArgs.getStatsClass();
        final String statsClasspath = spraynozzleArgs.getStatsClasspath();
        final String statsClassArgs = spraynozzleArgs.getStatsClassArgs();
        String[] topics = topic.split(",");
        if (topics.length == 1) {
            System.out.println("Listening to " + topic + " topic from " + zk + " and redirecting to " + url);
        } else {
            System.out.println("Listening to " + topic + " topics from " + zk + " and redirecting to " + url);
        }

        if (!buffering) {
            // Clear out zookeeper records so the spraynozzle drops messages between runs
            ZkClient zkClient = new ZkClient(zk, 10000);
            ZkUtils.deletePathRecursive(zkClient, "/consumers/kafka_spraynozzle_" + topic + cleanedUrl);
            while (ZkUtils.pathExists(zkClient, "/consumers/kafka_spraynozzle_" + topic + cleanedUrl)) {
                try {
                    Thread.sleep(250);
                } catch (java.lang.InterruptedException e) {
                    System.out.println("Sleep Exception!?");
                    e.printStackTrace();
                }
            }
        }

        // Kafka setup stuff
        Properties kafkaProps = new Properties();
        kafkaProps.put("zk.connect", zk);
        kafkaProps.put("zk.connectiontimeout.ms", "10000");
        kafkaProps.put("groupid", "kafka_spraynozzle_" + topic + cleanedUrl);
        kafkaProps.put("autooffset.reset", "largest");
        kafkaProps.put("fetch.size", String.valueOf(2*1024*1024));
        ConsumerConfig consumerConfig = new ConsumerConfig(kafkaProps);
        ConsumerConnector consumerConnector = Consumer.createJavaConsumerConnector(consumerConfig);
        HashMap<String, Integer> topicParallelism = new HashMap<String, Integer>();
        topicParallelism.put(topic, partitionCount);
        Map<String, List<KafkaStream<Message>>> topicMessageStreams = consumerConnector.createMessageStreams(topicParallelism);
        ArrayList<List<KafkaStream<Message>>> streams = new ArrayList<List<KafkaStream<Message>>>();
        for (int i = 0; i < topics.length; i++) {
            streams.add(topicMessageStreams.get(topics[i]));
        }
        ExecutorService executor = Executors.newFixedThreadPool(threadCount+(partitionCount*topics.length)+1);

        // Http Connection Pooling stuff
        final PoolingHttpClientConnectionManager cm = new PoolingHttpClientConnectionManager();
        cm.setMaxTotal(threadCount);
        cm.setDefaultMaxPerRoute(threadCount);

        // Message-passing queues within the spraynozzle
        final ConcurrentLinkedQueue<ByteArrayEntity> queue = new ConcurrentLinkedQueue<ByteArrayEntity>();
        final ConcurrentLinkedQueue<String> logQueue = new ConcurrentLinkedQueue<String>();

        // Build the worker threads
        StatsReporter statsReporter = getStatsReporter(statsClasspath, statsClass, statsClassArgs);
        executor.submit(new KafkaLog(logQueue, statsReporter, topic, url)); // TODO: Distinguish between the topics in the kafka logger

        for (List<KafkaStream<Message>> streamList : streams) {
            for (final KafkaStream<Message> stream : streamList) {
                executor.submit(new KafkaReader(queue, stream, logQueue));
            }
        }

        for (int i = 0; i < threadCount; i++) {
            // create new filter for every thread so the filters member variables are not shared
            KafkaFilter messageFilter = getKafkaFilter(filterClass, filterClasspath, filterClassArgs);
            executor.submit(new KafkaPoster(queue, cm, url, logQueue, messageFilter));
        }
    }

    private static KafkaFilter getKafkaFilter(String filterClass, String filterClasspath, String filterClassArgs){
        KafkaFilter messageFilter = (KafkaFilter)getClass(filterClasspath, filterClass, filterClassArgs);
        if (messageFilter == null) {
            messageFilter = new KafkaNoopFilter();
        }
        System.out.println("Using message filter: " + messageFilter);
        return messageFilter;
    }

    private static StatsReporter getStatsReporter(String classpath, String className, String classArgs){
        StatsReporter statsReporter = (StatsReporter)getClass(classpath, className, classArgs);
        if (statsReporter == null) {
            statsReporter = new NoopStatsReporter();
        }
        System.out.println("Using stats reporter: " + statsReporter);
        return statsReporter;
    }

    private static Object getClass(String classpath, String className, String classArgs){
        Object newClass = null;
        if (classpath != null && className != null) {
            File file = new File(classpath);
            try {
                URL[] urls = new URL[]{file.toURL()};
                ClassLoader cl = new URLClassLoader(urls);
                Class<?> cls = cl.loadClass(className);
                if (classArgs == null) {
                    newClass = cls.newInstance();
                } else {
                    newClass = cls.getConstructor(String.class).newInstance(classArgs);
                }
            } catch (MalformedURLException e) {
                System.out.println("Bad classpath provided: " + classpath);
            } catch (ClassNotFoundException e) {
                System.out.println("Class not found: " + className);
            } catch (InstantiationException e) {
                System.out.println("Cannot create instance of class: " + className);
            } catch (IllegalAccessException e) {
                System.out.println("Class did I have no idea what but its bad and illegal: " + className);
            } catch (NoSuchMethodException e) {
                System.out.println("Class with custom arguments must have a constructor taking 1 String: " + className);
            } catch (InvocationTargetException e) {
                System.out.println("Error initializing custom class: " + className + " " + e.getMessage());
            } catch (Exception e) {
                System.out.println("Unknown error initializing custom class: " + className + " " + e.getMessage());
            }
        }
        return newClass;
    }
}