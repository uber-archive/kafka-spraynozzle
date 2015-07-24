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
    private DistributedAtomicInteger leaderElections;

    public SpraynozzleLeaderLatch(String connString, String latchPath, String counterPath, String id) {
        zkClient = CuratorFrameworkFactory.newClient(connString, new ExponentialBackoffRetry(1000, 3));
        this.id = id;
        this.latchPath = latchPath;
        this.counterPath = counterPath;
    }

    public void start() throws Exception {
        zkClient.start();
        zkClient.blockUntilConnected();
        leaderLatch = new LeaderLatch(zkClient, latchPath, id);
        leaderElections = new DistributedAtomicInteger(zkClient, counterPath, new ExponentialBackoffRetry(1000, 3));
        leaderLatch.start();
    }

    public Boolean isFirstLeader() throws Exception {
        AtomicValue<Integer> value = leaderElections.increment();
        while (value.succeeded()) { // busy waiting
            System.out.println("leaderElections increment succeeded");
            //System.out.println("Leader elections preValue: " + value.preValue());
            if (value.preValue() == 0) {
                return true;
            } else {
                return false;
            }
        }
        return false; // never runs
    }

    public void blockUntilisLeader() throws Exception {
        leaderLatch.await();
    }

    public Participant currentLeader() throws Exception {
        return leaderLatch.getLeader();
    }
}
