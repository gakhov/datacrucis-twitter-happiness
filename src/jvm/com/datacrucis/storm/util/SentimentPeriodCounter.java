package com.datacrucis.storm.util;

import java.io.Serializable;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;

import com.datacrucis.storm.util.SentimentCounter;


/**
 * Special type of Counter that supports period counters over sentiment counters.
 */
public final class SentimentPeriodCounter implements Serializable {

    private Map<String, SentimentCounter> sentimentCountersByPeriod = new HashMap<String, SentimentCounter>();

    public SentimentPeriodCounter(Set<String> supportedPeriodNames) {
        for (String period : supportedPeriodNames) {
            sentimentCountersByPeriod.put(period, new SentimentCounter());
        }
    }

    private boolean isValidPeriod(String period) {
        return sentimentCountersByPeriod.containsKey(period);
    }

    public void increment(String sentimentClass) {
        for (String period : sentimentCountersByPeriod.keySet()) {
            sentimentCountersByPeriod.get(period).increment(sentimentClass);
        }
    }

    public Map<String, Integer> getAllForPeriod(String period) {
        if(!isValidPeriod(period)) {
            throw new IllegalArgumentException("Unexpected period: " + period);
        }
        return sentimentCountersByPeriod.get(period).getAll();
    }

    public void reset(String period) {
        if(!isValidPeriod(period)) {
            throw new IllegalArgumentException("Unexpected period" + period);
        }
        sentimentCountersByPeriod.get(period).reset();
    }

    public void resetAll() {
        for(String period : sentimentCountersByPeriod.keySet()) {
            sentimentCountersByPeriod.get(period).reset();
        }
    }
}
