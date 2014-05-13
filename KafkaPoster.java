import java.util.Date;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.util.EntityUtils;

public class KafkaPoster implements Runnable {
    ConcurrentLinkedQueue<ByteArrayEntity> queue;
    PoolingHttpClientConnectionManager cm;
    String url;

    public KafkaPoster(ConcurrentLinkedQueue<ByteArrayEntity> queue, PoolingHttpClientConnectionManager cm, String url) {
        this.queue = queue;
        this.cm = cm;
        this.url = url;
    }

    public void run() {
        long threadId = Thread.currentThread().getId();
        System.out.println("Starting thread " + threadId);
        CloseableHttpClient client = HttpClientBuilder.create().setConnectionManager(this.cm).build();
        long lastReconnect = new Date().getTime();
        while(true) {
            ByteArrayEntity jsonEntity = this.queue.poll();
            if(jsonEntity != null) {
                try {
                    System.out.println("Posting message");
                    HttpPost post = new HttpPost(this.url);
                    post.setHeader("User-Agent", "KafkaSpraynozzle-0.0.1");
                    post.setEntity(jsonEntity);
                    CloseableHttpResponse response = client.execute(post);
                    System.out.println("Response code: " + response.getStatusLine().getStatusCode());
                    long currentTime = new Date().getTime();
                    if(currentTime - lastReconnect > 10000) {
                        lastReconnect = currentTime;
                        response.close();
                    } else {
                        EntityUtils.consume(response.getEntity());
                    }
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
}
