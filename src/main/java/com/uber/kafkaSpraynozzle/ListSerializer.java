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

    private static void writeIntToByteStream(int val, ByteArrayOutputStream stream){
        byte[] buf = {0,0,0,0};
        buf[3] = (byte) (val       );
        buf[2] = (byte) (val >>>  8);
        buf[1] = (byte) (val >>> 16);
        buf[0] = (byte) (val >>> 24);
        stream.write(buf, 0, 4);
    }

    private static int readIntFromByteStream(ByteArrayInputStream stream) throws IOException{
        byte[] buf = {0,0,0,0};
        int actualRead = stream.read(buf, 0, 4);
        if (actualRead != 4){
            throw new IOException("Deserialized data corrupted");
        }
        return getInt(buf);
    }

    private static int getInt(byte[] b) {
        return ((b[3] & 0xFF)      ) +
                ((b[2] & 0xFF) <<  8) +
                ((b[1] & 0xFF) << 16) +
                ((b[0]       ) << 24);
    }

    /**
     * Serialize the List\<ByteArrayEntitity\> object into an byte array so it can be posted in one call
     * The encoding rule is plain simple:
     *  {totalElements}{length}{byte block of length}{length}{byte block of length}...
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
        writeIntToByteStream(entities.size(), serialized);
        for (int i = 0; i < entities.size(); ++i){
            writeIntToByteStream((int) entities.get(i).getContentLength(), serialized);
            entities.get(i).writeTo(serialized);
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
        ByteArrayInputStream input = new ByteArrayInputStream(serialized);
        final long largest_block = 1024L*1024L*128L;

        int arraySize = readIntFromByteStream(input);
        List<ByteArrayEntity> retVal = new ArrayList<ByteArrayEntity>(arraySize);
        for (int i = 0; i < arraySize; ++i){
            int objectSize = readIntFromByteStream(input);
            if (objectSize >= largest_block){ // >=128MB is dangerous
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
