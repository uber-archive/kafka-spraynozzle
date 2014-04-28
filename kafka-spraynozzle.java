import java.util.ArrayList;
import java.util.List;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.HttpClient;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.message.BasicNameValuePair;

class KafkaSpraynozzle {
    public static void main(String[] args) {
        String topic = args[0];
        String url = args[1];
        System.out.println("Listening to " + topic + " topic and redirecting to " + url + " (not really)");
        HttpClient client = HttpClientBuilder.create().build();
        HttpPost post = new HttpPost(url);
        post.setHeader("User-Agent", "Spraynozzle 0.0.1");
    }
}