package com.uber.kafkaSpraynozzle.benchmark;

import org.apache.http.entity.ByteArrayEntity;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * This simulator is just taking out queue items, and throws them away.
 * If needed it can simulate the delay in post.
 * Created by xdong on 11/16/15.
 */
public class KafkaPosterSimulator implements Runnable {
    public KafkaPosterSimulator(int postCostInMicroseconds, ConcurrentLinkedQueue<ByteArrayEntity> queue,
                                AtomicBoolean abortFlag){
        this.postCostInMicroseconds = postCostInMicroseconds;
        this.queue = queue;
        this.abort = abortFlag;
    }
    private AtomicBoolean abort = new AtomicBoolean(false);
    private long postCostInMicroseconds = 1L;
    private ConcurrentLinkedQueue<ByteArrayEntity> queue;
    private final long NANO_PER_SECOND = 1000L*1000L*1000L;
    private final long PRINT_INTERVAL_IN_SEC = 5; /// every 5 seconds print one stats

    private static void sleepNanoSeconds(long nanoSecondsSleep){
        long start = System.nanoTime();
        long end;
        do{
            end = System.nanoTime();
            Thread.yield();
        }while(start + nanoSecondsSleep >= end);
    }

    public void run() {
        long startNanoTime = System.nanoTime();
        long simulatedPostCount = 0L;
        long sleepInNano = 1000;
        long lastReportNanoTime = startNanoTime;
        long threadId = Thread.currentThread().getId();

        while(!abort.get()){
            ByteArrayEntity entity = queue.poll();
            long currentNanoTime = System.nanoTime();
            long overallNanoTime = (currentNanoTime - startNanoTime + 1);
            if (entity != null) {
                simulatedPostCount += 1;
                long simulatedCostInMilli = overallNanoTime / 1000 / simulatedPostCount;
                while (simulatedCostInMilli < postCostInMicroseconds){
                    sleepNanoSeconds(sleepInNano);
                    currentNanoTime = System.nanoTime();
                    overallNanoTime = (currentNanoTime - startNanoTime + 1);
                    simulatedCostInMilli = overallNanoTime / 1000 / simulatedPostCount;
                }
            }
            if (currentNanoTime - lastReportNanoTime > PRINT_INTERVAL_IN_SEC * NANO_PER_SECOND){
                lastReportNanoTime = currentNanoTime;
                System.out.println(String.format("Post thread %d: Posting at %d message/sec.", threadId,
                        simulatedPostCount * NANO_PER_SECOND/overallNanoTime));

            }
        }
    }
}
