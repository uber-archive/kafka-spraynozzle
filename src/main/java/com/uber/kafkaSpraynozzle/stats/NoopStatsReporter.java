package com.uber.kafkaSpraynozzle.stats;

/**
 * No-op stats reporter
 */
public class NoopStatsReporter implements StatsReporter {
    @Override
    public void count(String stat, long delta) {
        // NO-OP
    }
}
