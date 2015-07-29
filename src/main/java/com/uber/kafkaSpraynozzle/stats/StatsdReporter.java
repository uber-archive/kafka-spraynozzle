package com.uber.kafkaSpraynozzle.stats;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.timgroup.statsd.NonBlockingStatsDClient;
import com.timgroup.statsd.StatsDClient;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Statsd StatsReporter implementation
 *
 * To initialize this class you need to provide it with the following json string:
   {
       "host": <hostname>,
       "port": <port>,
       "statsPrefix": <prefix for all stats> (optional)
   }
 */
public class StatsdReporter implements StatsReporter {
    private final StatsDClient statsd;
    private final String statsPrefix;
    private final String statsdHost;
    private final Integer statsdPort;

    public StatsdReporter(String jsonConfig) throws IOException {
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
        this.statsd = new NonBlockingStatsDClient(this.statsPrefix, this.statsdHost, this.statsdPort);
    }

    @Override
    public void count(String stat, long delta){
        try {
            statsd.count(stat, delta);
        } catch(Exception e){
            System.out.println("There was an error reporting stat \'" + stat + "\' to StatsD: " + e.getMessage());
        }
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
