import java.util.Properties;
import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.lang.Runnable;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.consumer.KafkaStream;
import kafka.message.Message;
import kafka.message.MessageAndMetadata;
import kafka.utils.Utils;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.HttpClientBuilder;

class KafkaSpraynozzle {
    public static void main(String[] args) {
        String topic = args[0];
        final String url = args[1];
        String zk = args[2];
        System.out.println("Listening to " + topic + " topic from " + zk + " and redirecting to " + url + " (not really)");

        final HttpClient client = HttpClientBuilder.create().build();
        Properties kafkaProps = new Properties();
        kafkaProps.put("zk.connect", zk);
        kafkaProps.put("zk.connectiontimeout.ms", "10000");
        kafkaProps.put("groupid", "kafka_spraynozzle");
        ConsumerConfig consumerConfig = new ConsumerConfig(kafkaProps);
        ConsumerConnector consumerConnector = Consumer.createJavaConsumerConnector(consumerConfig);
        HashMap<String, Integer> topicParallelism = new HashMap<String, Integer>();
        topicParallelism.put(topic, 4);
        Map<String, List<KafkaStream<Message>>> topicMessageStreams = consumerConnector.createMessageStreams(topicParallelism);
        List<KafkaStream<Message>> streams = topicMessageStreams.get(topic);
        ExecutorService executor = Executors.newFixedThreadPool(4);

        for(final KafkaStream<Message> stream: streams) {
            executor.submit(new Runnable() {
                public void run() {
                    for(MessageAndMetadata msgAndMetadata: stream) {
                        HttpPost post = new HttpPost(url);
                        post.setHeader("User-Agent", "KafkaSpraynozzle-0.0.1");
			try {
                            StringEntity jsonEntity = new StringEntity(new String(((Message)msgAndMetadata.message()).payload().array(), "UTF-8"), ContentType.APPLICATION_JSON);
                            post.setEntity(jsonEntity);
                            HttpResponse response = client.execute(post);
                            System.out.println("Response code: " + response.getStatusLine().getStatusCode());
                        } catch (java.io.UnsupportedEncodingException e) {
                            e.printStackTrace();
                        } catch (java.io.IOException e) {
                            e.printStackTrace();
                        }
                    }
                }
            });
        }
    }
}
