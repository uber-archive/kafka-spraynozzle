
package com.uber.kafkaSpraynozzle;

import kafka.common.InvalidMessageSizeException;
import org.apache.http.entity.ByteArrayEntity;
import org.junit.Test;


import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by xdong on 11/17/15.
 */
public class ListSerializerTest {

    @Test
    public void SimpleCaseShouldWork(){
        int arrayLength = 10;
        List<ByteArrayEntity> entities = new ArrayList<ByteArrayEntity>(arrayLength);
        for (int i = 0; i < arrayLength; ++i){
            int elementLength = i * 10;
            ByteArrayOutputStream helper = new ByteArrayOutputStream();
            for (int j = 0; j < elementLength; ++j){
                helper.write(i * 1000 + j);
            }
            ByteArrayEntity entity = new ByteArrayEntity(helper.toByteArray());
            entities.add(entity);
        }
        ByteArrayEntity serialized = null;
        try {
            serialized = ListSerializer.toByteArrayEntity(entities);
        }catch(InvalidMessageSizeException ex){
            assert("corrupted message size".equals(ex.getMessage()));
        }catch(IOException ex){
            assert("IO exception during exception".equals(ex.getMessage()));
        }
        assert(serialized != null && serialized.getContentLength() > 100L);

        byte[] payload = ListSerializer.getByteArrayEntityBytes(serialized);

        List<ByteArrayEntity> deserialized = null;
        try {
            deserialized = ListSerializer.toListOfByteArrayEntity(payload);
        }catch(IOException ex){
            assert("IO exception during exception".equals(ex.getMessage()));
        }
        assert(deserialized != null && deserialized.size() == entities.size());
        for (int i = 0; i < entities.size(); ++i){
            assert(entities.get(i).getContentLength() == deserialized.get(i).getContentLength());
            byte[] originalBytes = ListSerializer.getByteArrayEntityBytes(entities.get(i));
            byte[] pipedBytes = ListSerializer.getByteArrayEntityBytes(deserialized.get(i));
            assert(originalBytes.length == pipedBytes.length);
            for (int j = 0; j < originalBytes.length; ++j){
                assert(originalBytes[j] == pipedBytes[j]);
            }
        }
    }
    @Test
    public void ExceptionForCorruptedData(){
        int arrayLength = 10;
        List<ByteArrayEntity> entities = new ArrayList<ByteArrayEntity>(arrayLength);
        for (int i = 0; i < arrayLength; ++i){
            int elementLength = i * 10;
            ByteArrayOutputStream helper = new ByteArrayOutputStream();
            for (int j = 0; j < elementLength; ++j){
                helper.write(i * 1000 + j);
            }
            ByteArrayEntity entity = new ByteArrayEntity(helper.toByteArray());
            entities.add(entity);
        }
        ByteArrayEntity serialized = null;
        try {
            serialized = ListSerializer.toByteArrayEntity(entities);
        }catch(InvalidMessageSizeException ex){
            assert("corrupted message size".equals(ex.getMessage()));
        }catch(IOException ex){
            assert("IO exception during exception".equals(ex.getMessage()));
        }
        assert(serialized != null && serialized.getContentLength() > 100L);

        byte[] payload = ListSerializer.getByteArrayEntityBytes(serialized);
        for (int i = 0; i < payload.length; ++i){
            payload[i] += i;
        }
        boolean exceptionThrown = false;
        List<ByteArrayEntity> deserialized = null;
        try {
            deserialized = ListSerializer.toListOfByteArrayEntity(payload);
        }catch(IOException ex){
            exceptionThrown = true;

        }catch(InvalidMessageSizeException ex){
            exceptionThrown = true;
        }
        assert(exceptionThrown);
        assert(deserialized == null);
    }
}
