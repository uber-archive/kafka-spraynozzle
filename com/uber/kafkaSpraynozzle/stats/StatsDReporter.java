package com.uber.kafkaSpraynozzle.stats;

import com.timgroup.statsd.NonBlockingStatsDClient;
import com.timgroup.statsd.StatsDClient;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Statsd StatsReporter implementation
 *
 * To initialize this class you need to provide it the path to a .properties file containing:
 * statsd.host - the statsd host
 * statsd.port - the statsd port
 * statsd.statsPrefix (optional) - a prefix for all stat names
 */
public class StatsdReporter implements StatsReporter {
    private final StatsDClient statsd;
    private final String statsPrefix;
    private final String statsdHost;
    private final Integer statsdPort;

    public StatsdReporter(String propertyFilePath) throws IOException {
        Properties statsProperties = new Properties();
        InputStream fileInputStream;
        try {
            fileInputStream = new FileInputStream(propertyFilePath);
            statsProperties.load(fileInputStream);
            fileInputStream.close();
        } catch (FileNotFoundException e) {
            System.out.println("Failed to find property file: " + propertyFilePath);
            throw e;
        } catch (IOException e) {
            System.out.println("Failed to load properties: " + e.getMessage());
            throw e;
        }

        this.statsPrefix = statsProperties.getProperty("statsd.statsPrefix", "");
        this.statsdHost = statsProperties.getProperty("statsd.host");
        this.statsdPort = Integer.valueOf(statsProperties.getProperty("statsd.port"));
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
