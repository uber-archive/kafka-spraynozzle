import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.HttpClientBuilder;

class KafkaSpraynozzle {
    public static void main(String[] args) {
        String topic = args[0];
        String url = args[1];
        System.out.println("Listening to " + topic + " topic and redirecting to " + url + " (not really)");
        HttpClient client = HttpClientBuilder.create().build();
        HttpPost post = new HttpPost(url);
        post.setHeader("User-Agent", "KafkaSpraynozzle-0.0.1");
        StringEntity fakeJsonEntity = new StringEntity("{\"hello\": \"world\"}", ContentType.APPLICATION_JSON);
        post.setEntity(fakeJsonEntity);
        try {
            HttpResponse response = client.execute(post);
            System.out.println("Response code: " + response.getStatusLine().getStatusCode());
        } catch (java.io.IOException e) {
            e.printStackTrace();
        }
    }
}