package com.uber.kafkaSpraynozzle.stats;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.timgroup.statsd.NonBlockingStatsDClient;
import com.timgroup.statsd.StatsDClient;

import java.io.IOException;
import java.util.HashMap;

/**
 * Statsd StatsReporter implementation
 */
public class StatsdReporter implements StatsReporter {
    private final StatsDClient statsd;
    private final String statsPrefix;
    private final String statsdHost;
    private final Integer statsdPort;

    public StatsdReporter(String jsonString) {
        // convert arguments json into dictionary
        HashMap<String, Object> jsonMap;
        try {
            jsonMap = new ObjectMapper().readValue(jsonString, new TypeReference<HashMap<String, Object>>() {});
        } catch (IOException e) {
            throw new IllegalArgumentException("input is not json: " + jsonString);
        }

        this.statsPrefix = (String)jsonMap.get("statsPrefix");
        this.statsdHost = (String)jsonMap.get("host");
        this.statsdPort = (Integer)jsonMap.get("port");
        System.out.println("Connecting to statsd at: " + this.statsdHost + ":" + this.statsdPort);
        this.statsd = new NonBlockingStatsDClient(this.statsPrefix, this.statsdHost, this.statsdPort);
    }

    @Override
    public void count(String stat, long delta){
        statsd.count(stat, delta);
    }

    @Override
    public String toString() {
        return "StatsdReporter{" +
                "stats prefix=" + this.statsPrefix +
                ", statsd host=" + this.statsdHost +
                ", statsd port=" + this.statsdPort +
                "}";
    }
}
