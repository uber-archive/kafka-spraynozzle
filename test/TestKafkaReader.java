import java.util.ConcurrentLinkedQueue;
import org.junit.Test;
import org.junit.Assert;
import org.apache.http.entity.ByteArrayEntity;
import kafka.consumer.KafkaStream;
import kafka.message.Message;

public class TestKafkaReader {
    @Test public void readerConstruction() {
        ConcurrentLinkedQueue<ByteArrayEntity> queue = new ConcurrentLinkedQueue<ByteArrayEntity>();
        KafkaStream<Message> stream = new KafkaStream<Message>();
