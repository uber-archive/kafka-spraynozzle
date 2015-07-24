package com.uber.kafkaSpraynozzle;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.framework.recipes.atomic.DistributedAtomicInteger;
import org.apache.curator.framework.recipes.atomic.AtomicValue;

public class ClearLeaderElections {
    // ClearLeaderElections [zkConnectionString] [counterPath in zk]
    public static void main(String[] args) throws Exception {
        CuratorFramework zkClient = CuratorFrameworkFactory.newClient(args[0], new ExponentialBackoffRetry(1000, 3));
        String counterPath = args[1];
        DistributedAtomicInteger leaderElections = new DistributedAtomicInteger(zkClient, counterPath, new ExponentialBackoffRetry(1000, 3));
        if (!leaderElections.initialize(0)) {
            leaderElections.forceSet(0);
        }
    }
}
