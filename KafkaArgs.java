import com.lexicalscope.jewel.cli.CommandLineInterface;
import com.lexicalscope.jewel.cli.Option;
import com.lexicalscope.jewel.cli.Unparsed;

@CommandLineInterface(application="kafka-spraynozzle")
public interface KafkaArgs {
    @Unparsed(name="topic", description="The kafka topic to stream from") String getTopic();
    @Option(shortName="u", description="The URL to post kafka messages to") String getUrl();
    @Option(shortName="z", description="The Zookeeper instance to read from") String getZk();
    @Option(shortName="n", description="The number of HTTP posting threads to use") int getThreadCount();
    @Option(shortName="p", defaultValue="1", description="The number of topic partitions (default: 1)") int getPartitionCount();
    @Option(shortName="h", helpRequest=true, description="Display this Help message") boolean getHelp();
}