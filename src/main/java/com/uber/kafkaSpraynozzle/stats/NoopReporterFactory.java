package com.uber.kafkaSpraynozzle.stats;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import com.fasterxml.jackson.annotation.JsonTypeName;

import java.io.IOException;

@JsonTypeName("noop")
public class NoopReporterFactory implements StatsReporterFactory {
    public NoopReporterFactory() {
    }

    @Override
    public ScheduledReporter build(MetricRegistry registry) {
        return null;
    }

    @Override
    public String toString() {
        return "NoopReporterFactory";
    }
}
