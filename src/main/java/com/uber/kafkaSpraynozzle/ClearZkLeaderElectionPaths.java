package com.uber.kafkaSpraynozzle;

import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;

/*
* Clear out zookeeper records related to leader elections between deploys
*/
public class ClearZkLeaderElectionPaths {
    // ClearLeaderElections [zkConnectionString] [counterPath in zk] [leaderLatchPath in zk]
    // consumers/kafka_spraynozzle_leader_elections
    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.out.println("Proper arguments are: [zkConnectionString], [counterPath in zk] and [leaderLatchPath in zk]");
            System.exit(0);
        }
        String zk = args[0];
        String counterPath = args[1];
        String leaderLatchPath = args[2];
        ZkClient zkClient = new ZkClient(zk, 10000);
        ZkUtils.deletePathRecursive(zkClient, counterPath);
        ZkUtils.deletePathRecursive(zkClient, leaderLatchPath);
        while (ZkUtils.pathExists(zkClient, counterPath)) {
            try {
                Thread.sleep(250);
            } catch (java.lang.InterruptedException e) {
                System.out.println("Sleep Exception!?");
                e.printStackTrace();
            }
        }
        while (ZkUtils.pathExists(zkClient, leaderLatchPath)) {
            try {
                Thread.sleep(250);
            } catch (java.lang.InterruptedException e) {
                System.out.println("Sleep Exception!?");
                e.printStackTrace();
            }
        }
    }
}
