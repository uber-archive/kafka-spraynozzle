package com.uber.kafkaSpraynozzle.stats;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.readytalk.metrics.StatsDReporter;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

@JsonTypeName("statsd")
public class StatsdReporterFactory implements StatsReporterFactory {
    private final String statsPrefix;
    private final String statsdHost;
    private final Integer statsdPort;

    public StatsdReporterFactory(String jsonConfig) throws IOException {
        // convert jsonConfig into Map
        Map<String, Object> configMap;
        try {
            configMap = new ObjectMapper().readValue(jsonConfig, new TypeReference<HashMap<String, Object>>() {});
        } catch (IOException e) {
            throw new IllegalArgumentException("input is not json: " + jsonConfig);
        }

        this.statsPrefix = (String)configMap.get("statsPrefix");
        this.statsdHost = (String)configMap.get("host");
        this.statsdPort = (Integer)configMap.get("port");
        System.out.println("Connecting to statsd at: " + this.statsdHost + ":" + this.statsdPort);
    }

    @Override
    public ScheduledReporter build(MetricRegistry registry) {
        return StatsDReporter.forRegistry(registry)
                .prefixedWith(statsPrefix)
                .build(statsdHost, statsdPort);
    }

    @Override
    public String toString() {
        return "StatsdReporterFactory{" +
                "stats prefix=" + this.statsPrefix +
                ", statsd host=" + this.statsdHost +
                ", statsd port=" + this.statsdPort +
                "}";
    }
}
