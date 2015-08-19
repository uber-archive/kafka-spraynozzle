package com.uber.kafkaSpraynozzle;

import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;

/*
* Clear out zookeeper records related to leader elections between deploys
*/
public class ClearZkLeaderElectionPaths {
    // ClearLeaderElections [zkConnectionString] [topic] [url]
    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.out.println("Proper arguments are: [zkConnectionString] [topic] [url]");
            System.exit(0);
        }
        String zk = args[0];
        String topic = args[1];
        String url = args[2];
        String[] topics = topic.split(",");
        String cleanedUrl = url.replaceAll("[/\\:]", "_");
        String zkLeaderLatchFolderPath = "/consumers/kafka_spraynozzle_leader_latch_" + topics[0] + cleanedUrl;
        String zkLeaderElectionFolderPath = "/consumers/kafka_spraynozzle_leader_elections_" + topics[0] + cleanedUrl;
        ZkClient zkClient = new ZkClient(zk, 10000);
        ZkUtils.deletePathRecursive(zkClient, zkLeaderLatchFolderPath);
        ZkUtils.deletePathRecursive(zkClient, zkLeaderElectionFolderPath);
        while (ZkUtils.pathExists(zkClient, zkLeaderLatchFolderPath)) {
            try {
                Thread.sleep(250);
            } catch (java.lang.InterruptedException e) {
                System.out.println("Sleep Exception!?");
                e.printStackTrace();
            }
        }
        while (ZkUtils.pathExists(zkClient, zkLeaderElectionFolderPath)) {
            try {
                Thread.sleep(250);
            } catch (java.lang.InterruptedException e) {
                System.out.println("Sleep Exception!?");
                e.printStackTrace();
            }
        }
    }
}
