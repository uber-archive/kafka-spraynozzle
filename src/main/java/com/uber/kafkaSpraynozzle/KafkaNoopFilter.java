package com.uber.kafkaSpraynozzle;

import org.apache.http.entity.ByteArrayEntity;

public class KafkaNoopFilter implements KafkaFilter {
    public KafkaNoopFilter() { }
    public ByteArrayEntity filter(ByteArrayEntity jsonEntity) {
        return jsonEntity;
    }
}
