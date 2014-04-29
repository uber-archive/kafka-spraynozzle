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

class KafkaSpraynozzle {
    public static void main(String[] args) {
        final Integer threadCount = 4;
        String topic = args[0];
        final String url = args[1];
        String zk = args[2];
        System.out.println("Listening to " + topic + " topic from " + zk + " and redirecting to " + url + " (not really)");

        Properties kafkaProps = new Properties();
        kafkaProps.put("zk.connect", zk);
        kafkaProps.put("zk.connectiontimeout.ms", "10000");
        kafkaProps.put("groupid", "kafka_spraynozzle");
        ConsumerConfig consumerConfig = new ConsumerConfig(kafkaProps);
        ConsumerConnector consumerConnector = Consumer.createJavaConsumerConnector(consumerConfig);
        HashMap<String, Integer> topicParallelism = new HashMap<String, Integer>();
        topicParallelism.put(topic, threadCount);
        Map<String, List<KafkaStream<Message>>> topicMessageStreams = consumerConnector.createMessageStreams(topicParallelism);
        List<KafkaStream<Message>> streams = topicMessageStreams.get(topic);
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);

        for(final KafkaStream<Message> stream: streams) {
            executor.submit(new Runnable() {
                public void run() {
                    System.out.println("Starting thread");
                    CloseableHttpClient client = HttpClientBuilder.create().build();
                    for(MessageAndMetadata msgAndMetadata: stream) {
                        System.out.println("Processing message");
                        HttpPost post = new HttpPost(url);
                        post.setHeader("User-Agent", "KafkaSpraynozzle-0.0.1");
                        try {
                            ByteBuffer message = ((Message)msgAndMetadata.message()).payload();
                            Integer messageLen = ((Message)msgAndMetadata.message()).payloadSize();
                            Integer messageOffset = message.arrayOffset();
                            ByteArrayEntity jsonEntity = new ByteArrayEntity(message.array(), messageOffset, messageLen, ContentType.APPLICATION_JSON);
                            jsonEntity.setContentEncoding("UTF-8");
                            post.setEntity(jsonEntity);
                            CloseableHttpResponse response = client.execute(post);
                            System.out.println("Response code: " + response.getStatusLine().getStatusCode());
                            response.close();
                        } catch (java.io.UnsupportedEncodingException e) {
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
