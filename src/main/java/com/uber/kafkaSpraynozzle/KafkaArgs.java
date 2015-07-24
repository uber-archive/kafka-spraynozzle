package com.uber.kafkaSpraynozzle;

import com.lexicalscope.jewel.cli.CommandLineInterface;
import com.lexicalscope.jewel.cli.Option;
import com.lexicalscope.jewel.cli.Unparsed;

@CommandLineInterface(application="kafka-spraynozzle")
public interface KafkaArgs {
    @Unparsed(name="topic", description="The kafka topic to stream from") String getTopic();
    @Option(shortName="u", description="The URL to post kafka messages to (in the form http(s)://server.address(:port)/url)") String getUrl();
    @Option(shortName="z", description="The Zookeeper instance to read from (in the form server.address:port)") String getZk();
    @Option(shortName="n", description="The number of HTTP posting threads to use") int getThreadCount();
    @Option(shortName="b", description="Use Kafka protocol to buffer messages while spraynozzle is down (default: false)") boolean getBuffering();
    @Option(shortName="p", defaultValue="1", description="The number of topic partitions (default: 1)") int getPartitionCount();
    @Option(shortName="f", defaultToNull=true, description="The name of a class to use for filtering messages (default: none)") String getFilterClass();
    @Option(shortName="l", defaultToNull=true, description="The name of the leaderLatch zk path (default: none)") String getLeaderLatchZkPath();
    @Option(shortName="e", defaultToNull=true, description="The name of the distributed atomic counter zk path (default: none)") String getDistributedAtomicCounterZkPath();
    @Option(shortName="c", defaultToNull=true, description="The classpath to use for finding the above-mentioned filter class (default: none)") String getFilterClasspath();
    @Option(shortName="a", defaultToNull=true, description="The arguments for the above-mentioned filter class (default: none)") String getFilterClassArgs();
    @Option(shortName="s", defaultToNull=true, description="The name of a class to use for stats reporting (default: none)") String getStatsClass();
    @Option(shortName="w", defaultToNull=true, description="The classpath to use for finding the above-mentioned stats class (default: none)") String getStatsClasspath();
    @Option(shortName="d", defaultToNull=true, description="The arguments for the above-mentioned stats class (default: none)") String getStatsClassArgs();
    @Option(shortName="h", helpRequest=true, description="Display this Help message") boolean getHelp();
}
