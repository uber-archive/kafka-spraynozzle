package com.uber.kafkaSpraynozzle.filters;

import com.uber.kafkaSpraynozzle.KafkaFilter;
import org.apache.http.entity.ByteArrayEntity;

public class KafkaNoopFilter implements KafkaFilter {
    public KafkaNoopFilter() { }
    public boolean filter(ByteArrayEntity jsonEntity) {
        return true;
    }
}
