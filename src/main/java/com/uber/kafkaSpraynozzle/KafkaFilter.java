package com.uber.kafkaSpraynozzle;

import org.apache.http.entity.ByteArrayEntity;

public interface KafkaFilter {
    /**
     * Filter the incoming entity, returning the data to pass on or null to skip
     * @param jsonEntity the entity to potentially filter
     * @return the filtered data or null to skip
     */
    public ByteArrayEntity filter(ByteArrayEntity jsonEntity);
}
