package com.uber.kafkaSpraynozzle.benchmark;

import com.uber.kafkaSpraynozzle.KafkaReader;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * This class takes in one KafkaReader, and test the code path where it enqueues
 * It is driven by a thread.
 */
class KafkaReaderDriver implements Runnable {
    public KafkaReaderDriver(KafkaReader readerToTest, long packageCount,
                             int packageSize, int packagePerSecond, AtomicBoolean abortFlag){
        reader = readerToTest;
        sleepNanoInterval = 1000000000L/packagePerSecond;
        this.packageCount = packageCount;
        this.packageSize = packageSize;
        this.packagePerSecond = packagePerSecond;
        abort = abortFlag;
    }
    private KafkaReader reader;
    private long sleepNanoInterval = 10000; // 10 microseconds, 0.01 milli
    private long packageCount = 1000000;
    private int packageSize = 2000; // 2KB
    private int packagePerSecond = 1000;
    private final long NANO_PER_SECOND = 1000L*1000L*1000L;
    private final long PRINT_INTERVAL_IN_SEC = 5; /// every 5 seconds print one stats
    private AtomicBoolean abort = new AtomicBoolean(false);

    private static void sleepNanoSeconds(long nanoSecondsSleep){
        long start = System.nanoTime();
        long end;
        do{
            end = System.nanoTime();
            Thread.yield();
        }while(start + nanoSecondsSleep >= end);
    }

    public void run() {

        long threadId = Thread.currentThread().getId();
        System.out.println("Starting reader thread " + threadId);
        int pushCount = 0;
        byte[] bytes = new byte[packageSize];
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        long overallStartTime = System.nanoTime();
        long lastReportTime = overallStartTime;
        long totalSleepingNanoSeconds = 0L;
        for(long i = 0L; i < packageCount && !abort.get(); ++i) {
            ByteBuffer message = buffer;
            Integer messageLen = buffer.array().length;
            pushCount = reader.enqueueData(pushCount, messageLen, message);

            long currentEndTime = System.nanoTime();
            long totalElapsedTime = (currentEndTime - overallStartTime + 1); /// avoid division by zero
            long currentMessagePerSecond = (i + 1) * NANO_PER_SECOND / totalElapsedTime ;
            /// target our packagePerSecond goal here
            if ( currentMessagePerSecond > packagePerSecond ) {
                sleepNanoSeconds(sleepNanoInterval);
                totalSleepingNanoSeconds += sleepNanoInterval;
            }

            if (currentEndTime - lastReportTime > PRINT_INTERVAL_IN_SEC * NANO_PER_SECOND){
                System.out.println(String.format("Reader Thread %d: Sustained %d message/sec, %d KiB/sec. %d percent idle", threadId,
                        currentMessagePerSecond, currentMessagePerSecond * packageSize / 1000,
                        totalSleepingNanoSeconds * 100 / totalElapsedTime ));
                lastReportTime = currentEndTime;
            }
        }
    }

}