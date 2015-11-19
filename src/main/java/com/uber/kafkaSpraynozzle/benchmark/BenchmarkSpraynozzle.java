package com.uber.kafkaSpraynozzle.benchmark;

import com.codahale.metrics.MetricRegistry;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.http.*;

import static com.github.tomakehurst.wiremock.http.RequestMethod.POST;

import com.github.tomakehurst.wiremock.matching.RequestPattern;
import com.github.tomakehurst.wiremock.stubbing.StubMapping;
import com.lexicalscope.jewel.cli.ArgumentValidationException;
import com.lexicalscope.jewel.cli.CliFactory;
import com.uber.kafkaSpraynozzle.KafkaNoopFilter;
import com.uber.kafkaSpraynozzle.KafkaPoster;
import com.uber.kafkaSpraynozzle.KafkaReader;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by xdong on 11/16/15.
 * The benchmark tries to pump messages into the queue, and then either takes out the queue items directly,
 * or starts a real local http server to test the KafkaPoster thread.
 *
 * By this benchmark, we can get the throughput of enqueue, get the load balancing tested.
 *
 */
public class BenchmarkSpraynozzle {


    public static void main(String[] args) throws Exception {
        BenchmarkArgs benchmarkArgs;
        try {
            benchmarkArgs = CliFactory.parseArguments(BenchmarkArgs.class, args);
        } catch(ArgumentValidationException e) {
            System.err.println(e.getMessage());
            System.err.println(CliFactory.createCli(BenchmarkArgs.class).getHelpMessage());
            return;
        }

        System.out.print("\nTesting setting: \n");
        if (benchmarkArgs.getEnableRealHttp() == 1){
            System.out.print("Posting to endpoint, with endpoint[0] has 200ms delays.\n");
        }else{
            System.out.print("Skipping endpoint write.\n");
        }
        if (benchmarkArgs.getForceRoundRobin() == 1){
            System.out.print("Forcing round-robin.\n");
        }else{
            System.out.print("Least-response-time balancing.\n");
        }
        System.out.print(String.format("Packing %d items in post.\n", benchmarkArgs.getPackingPerPost()));
        System.out.println("\n");

        final long NANO_PER_SECOND = 1000L*1000L*1000L;

        ExecutorService executor = Executors.newFixedThreadPool(benchmarkArgs.getReaderThreads() + benchmarkArgs.getPostThreads() + 1);
        final ConcurrentLinkedQueue<ByteArrayEntity> queue = new ConcurrentLinkedQueue<ByteArrayEntity>();
        final ConcurrentLinkedQueue<String> logQueue = new ConcurrentLinkedQueue<String>();
        AtomicBoolean abortFlag = new AtomicBoolean(false);
        for (int i = 0; i < benchmarkArgs.getReaderThreads(); ++i){
            KafkaReader reader = new KafkaReader(new MetricRegistry(), queue, null);
            KafkaReaderDriver driver = new KafkaReaderDriver(reader, benchmarkArgs.getTestTime() * benchmarkArgs.getMessagePerPerSecond(),
                    benchmarkArgs.getMessageSize(), benchmarkArgs.getMessagePerPerSecond(), abortFlag);
            executor.submit(driver);
        }

        final long[] serverCalledCount = new long[benchmarkArgs.getHttpEndpointCount()];
        long startingTime = new Date().getTime();
        List<WireMockServer> servers = new ArrayList<WireMockServer>(benchmarkArgs.getHttpEndpointCount());
        if (benchmarkArgs.getEnableRealHttp() == 1){
            // Http Connection Pooling stuff
            List<String> urls = new ArrayList<String>(benchmarkArgs.getHttpEndpointCount());
            for (int i = 0; i < benchmarkArgs.getHttpEndpointCount(); ++i){
                urls.add("http://localhost:" + (18982 + i) + "/endpoint");
            }

            for (int i = 0; i < benchmarkArgs.getHttpEndpointCount(); ++i){
                servers.add(new WireMockServer(18982 + i));
                final int serverIdx = i;
                servers.get(i).addStubMapping(new StubMapping(new RequestPattern(POST, "/endpoint"), ResponseDefinition.ok()));
                servers.get(i).addMockServiceRequestListener(
                        new RequestListener() {
                            @Override
                            public void requestReceived(Request request, Response response) {
                                serverCalledCount[serverIdx]++;
                            }
                        }
                );
                servers.get(i).start();
                if (i == 0){ /// introduce imbalanced server response.
                    servers.get(0).addRequestProcessingDelay(200);
                }
            }
            final PoolingHttpClientConnectionManager cm = new PoolingHttpClientConnectionManager();
            cm.setMaxTotal(benchmarkArgs.getPostThreads());
            cm.setDefaultMaxPerRoute(benchmarkArgs.getPostThreads());

            for (int i = 0; i < benchmarkArgs.getPostThreads(); ++i) {
                KafkaPoster poster = new KafkaPoster(new MetricRegistry(), queue, cm, urls, new KafkaNoopFilter(),
                        2000, 1000, 20, benchmarkArgs.getForceRoundRobin() == 1, true);
                executor.submit(poster);
            }

        }else {
            for (int i = 0; i < benchmarkArgs.getPostThreads(); ++i) {
                KafkaPosterSimulator poster = new KafkaPosterSimulator(benchmarkArgs.getPostCost(), queue, abortFlag);
                executor.submit(poster);
            }
        }
        long startNanoTime = System.nanoTime();
        while (System.nanoTime() - startNanoTime < (benchmarkArgs.getTestTime() + 20)* NANO_PER_SECOND ){
            Thread.sleep(1000);
            if (benchmarkArgs.getEnableRealHttp() == 1) {
                long currentTime = new Date().getTime();
                if (currentTime - startingTime > 10 * 1000){
                    for (int i = 0; i < serverCalledCount.length; ++i){
                        System.out.println(String.format("Server %d, called %d times", i, serverCalledCount[i]));
                    }
                    startingTime = currentTime;
                }
            }
        }
        abortFlag.set(true);
        executor.shutdown();
        executor.awaitTermination(300, TimeUnit.SECONDS);
        for (WireMockServer server: servers){
            server.shutdown();
        }
    }
}