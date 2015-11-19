package com.uber.kafkaSpraynozzle;

import kafka.common.InvalidMessageSizeException;
import org.apache.http.entity.ByteArrayEntity;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
/**
 * Created by xdong on 11/17/15.
 * This serializer just serializes a list of ByteArrayEntities into one single ByteArrayEntities
 * Later it can be deserialized back.
 */
public class ListSerializer {

    /**
     * Serialize the List\<ByteArrayEntitity\> object into an byte array so it can be posted in one call
     * The encoding rule is plain simple:
     *  {totalElements}{length}{byte block of length}{length}{byte block of length}...
     * For example, a List of 3 ByteArrayEntities, with 10,20,30 bytes at length will deserialized into below:
     *  0x00000003, 0x00000001,[10 byte],0x00000002,[20 byte],0x00000003,[30 byte]
     * @param entities the List objects to serialize
     * @return the one single byte array, encoded.
     * @throws InvalidMessageSizeException If the array is too large to fit into one huge memory
     * @throws IOException if anything is wrong.
     */
    public static ByteArrayEntity toByteArrayEntity(List<ByteArrayEntity> entities) throws InvalidMessageSizeException, IOException {
        long totalLength = 0L;
        final long largest_block = 1024L*1024L*128L;
        for (int i = 0; i < entities.size(); ++i){
            totalLength += entities.get(i).getContentLength();
        }
        if (totalLength >= largest_block){ // >=128MB is dangerous
            throw new InvalidMessageSizeException("The batched messages are too large");
        }
        ByteArrayOutputStream serialized = new ByteArrayOutputStream();
        DataOutputStream output = new DataOutputStream(serialized);
        output.writeInt(entities.size());
        for (ByteArrayEntity entity : entities){
            output.writeInt((int) entity.getContentLength());
            entity.writeTo(output);
        }
        return new ByteArrayEntity(serialized.toByteArray());
    }

    /**
     * Deserialize the byte block back into our List<ByteArrayEntity> object
     * @param serialized the byte block
     * @return the deserialized object
     * @throws InvalidMessageSizeException if the data is corrupted, causing too huge data
     * @throws IOException if somehow stream read/write fails, or data corrupted
     */
    public static List<ByteArrayEntity> toListOfByteArrayEntity(byte[] serialized) throws InvalidMessageSizeException, IOException {
        ByteArrayInputStream byteStream = new ByteArrayInputStream(serialized);
        DataInputStream input = new DataInputStream(byteStream);
        final long largest_block = 1024L*1024L*128L;

        int arraySize = input.readInt();
        List<ByteArrayEntity> retVal = new ArrayList<ByteArrayEntity>(arraySize);
        for (int i = 0; i < arraySize; ++i){
            int objectSize = input.readInt();
            if (objectSize >= largest_block || objectSize < 0){ // >=128MB is dangerous
                throw new InvalidMessageSizeException("The batched messages are too large");
            }
            byte[] objectBytes = new byte[objectSize];
            int actualRead = input.read(objectBytes, 0, objectSize);
            if (actualRead != objectSize){
                throw new IOException("Deserialized entity corrupted");
            }
            ByteArrayEntity entity = new ByteArrayEntity(objectBytes);
            retVal.add(entity);
        }
        return retVal;
    }

    /**
     * Helper class for converting ByteArrayEntity to byte[] array
     * @param entity The byte array entity
     * @return The byte array copy
     */
    public static byte[] getByteArrayEntityBytes(ByteArrayEntity entity){
        ByteArrayOutputStream serialized = new ByteArrayOutputStream();
        try {
            entity.writeTo(serialized);
            return serialized.toByteArray();
        }catch(IOException ex){
            return new byte[0];
        }
    }

}
