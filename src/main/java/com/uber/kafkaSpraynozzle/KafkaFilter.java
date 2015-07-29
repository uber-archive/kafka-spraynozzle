package com.uber.kafkaSpraynozzle;

import org.apache.http.entity.ByteArrayEntity;

public interface KafkaFilter {
    public boolean filter(ByteArrayEntity jsonEntity);
}
