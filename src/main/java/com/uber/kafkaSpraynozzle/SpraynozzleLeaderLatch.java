package com.uber.kafkaSpraynozzle;

import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.framework.recipes.leader.Participant;
import org.apache.curator.framework.recipes.atomic.DistributedAtomicInteger;
import org.apache.curator.framework.recipes.atomic.AtomicValue;

public class SpraynozzleLeaderLatch {
    private CuratorFramework zkClient;
    private String latchPath;
    private String counterPath;
    private String id;
    private LeaderLatch leaderLatch;

    public SpraynozzleLeaderLatch(String connString, String latchPath, String id) {
        zkClient = CuratorFrameworkFactory.newClient(connString, new ExponentialBackoffRetry(1000, 3));
        this.id = id;
        this.latchPath = latchPath;
        this.counterPath = counterPath;
    }

    public void start() throws Exception {
        zkClient.start();
        zkClient.blockUntilConnected();
        leaderLatch = new LeaderLatch(zkClient, latchPath, id);
        leaderLatch.start();
    }

    public void blockUntilisLeader() throws Exception {
        leaderLatch.await();
    }

    public Participant currentLeader() throws Exception {
        return leaderLatch.getLeader();
    }
}
