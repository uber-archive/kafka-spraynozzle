package com.uber.kafkaSpraynozzle.filters;

import com.uber.kafkaSpraynozzle.KafkaFilter;
import org.apache.http.entity.ByteArrayEntity;

import java.util.Random;

/**
 * Filter which throttles incoming requests by dropping a percentage of the messages
 */
public class KafkaThrottlingFilter implements KafkaFilter{

    private double allowedPercentage;
    private Random random;

    /**
     * @param allowedPercentage - String between 0 and 1
     */
    public KafkaThrottlingFilter(String allowedPercentage) {
        this.allowedPercentage = Double.parseDouble(allowedPercentage);
        this.random = new Random();
    }

    @Override
    public boolean filter(ByteArrayEntity jsonEntity) {
        return random.nextDouble() < allowedPercentage;
    }

    @Override
    public String toString() {
        return "KafkaThrottlingFilter{allowedPercentage=" + allowedPercentage + '}';
    }
}
