package com.uber.kafkaSpraynozzle;

import com.lexicalscope.jewel.cli.CommandLineInterface;
import com.lexicalscope.jewel.cli.Option;
import com.lexicalscope.jewel.cli.Unparsed;
import java.util.List;

@CommandLineInterface(application="kafka-spraynozzle")
public interface KafkaArgs {
    @Unparsed(name="topic", description="The kafka topic to stream from") String getTopic();
    @Option(shortName="u", description="The URL(s) to post kafka messages to (in the form http(s)://server.address(:port)/url)") List<String> getUrls();
    @Option(shortName="z", description="The Zookeeper instance to read from (in the form server.address:port)") String getZk();
    @Option(shortName="n", description="The number of HTTP posting threads to use") int getThreadCount();
    @Option(shortName="b", description="Use Kafka protocol to buffer messages while spraynozzle is down (default: false)") boolean getBuffering();
    @Option(shortName="p", defaultValue="1", description="The number of topic partitions (default: 1)") int getPartitionCount();
    @Option(shortName="f", defaultToNull=true, description="The name of a class to use for filtering messages (default: none)") String getFilterClass();
    @Option(shortName="l", defaultToNull=false, description="Use zk to make the spraynozzle highly available (default: false)") Boolean getIsHighlyAvailable();
    @Option(shortName="c", defaultToNull=true, description="The classpath to use for finding the above-mentioned filter class (default: none)") String getFilterClasspath();
    @Option(shortName="a", defaultToNull=true, description="The arguments for the above-mentioned filter class (default: none)") String getFilterClassArgs();
    @Option(shortName="s", defaultToNull=true, description="The name of a class to use for stats reporting (default: none)") String getStatsClass();
    @Option(shortName="w", defaultToNull=true, description="The classpath to use for finding the above-mentioned stats class (default: none)") String getStatsClasspath();
    @Option(shortName="d", defaultToNull=true, description="The arguments for the above-mentioned stats class (default: none)") String getStatsClassArgs();
    @Option(longName="connectionTimeout", defaultToNull=true, description="The connection timeout in milliseconds for posting (default: none)") Integer getConnectionTimeout();
    @Option(longName="socketTimeout", shortName="t", defaultToNull=true, description="The socket timeout in milliseconds for posting (default: none)") Integer getSocketTimeout();
    @Option(longName="help", shortName="h", helpRequest=true, description="Display this Help message") boolean getHelp();
    @Option(shortName="i", defaultValue = "1", description="Number of messages to pack as a batch before POST to endpoint") int getBatchSize();
    @Option(shortName="e", description="Enable Least-Response-Time load balancing when posting") boolean getEnableBalancing();
}
