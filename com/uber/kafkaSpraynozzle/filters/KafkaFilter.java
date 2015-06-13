package com.uber.kafkaSpraynozzle.filters;

import org.apache.http.entity.ByteArrayEntity;

public interface KafkaFilter {
    public boolean filter(ByteArrayEntity jsonEntity);
}
