package com.uber.kafkaSpraynozzle.stats;

/**
 * Interface for sending statistical data
 */
public interface StatsReporter {
    /**
     * increment the given counter
     * @param stat
     *     the name of the counter to adjust
     * @param delta
     *     the amount to adjust the counter by
     */
    void count(String stat, long delta);
}
