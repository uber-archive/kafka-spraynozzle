package com.uber.kafkaSpraynozzle.benchmark;

import com.lexicalscope.jewel.cli.CommandLineInterface;
import com.lexicalscope.jewel.cli.Option;

/**
 * Created by xdong on 11/16/15.
 */
@CommandLineInterface(application="kafka-spraynozzle-benchmark")
public interface BenchmarkArgs {
    @Option(shortName="rt", defaultValue = "4", description="Reader thread count")
    int getReaderThreads();
    @Option(shortName="pt", defaultValue = "4", description="Post thread count")
    int getPostThreads();
    @Option(shortName="mps", defaultValue = "200000", description="Kafka Message Per Second, Per thread")
    int getMessagePerPerSecond();
    @Option(shortName="size", defaultValue = "2240", description="Kafka Message Size, in bytes")
    int getMessageSize();
    @Option(shortName="pc", defaultValue = "10", description="Post cost in micro-seconds (1/1000 of milliseconds)")
    int getPostCost();
    @Option(shortName="t", defaultValue = "90", description="Test time in seconds")
    int getTestTime();
    @Option(shortName="e", defaultValue = "1", description="enable real http test")
    int getEnableRealHttp();
    @Option(shortName="n", defaultValue = "4", description="http endpoint count")
    int getHttpEndpointCount();
    @Option(shortName="f", defaultValue = "0", description="Force round robin post instead of least-response time")
    int getForceRoundRobin();
    @Option(shortName="s", defaultValue = "20", description="How many messages to do in one batch")
    int getPackingPerPost();

}
