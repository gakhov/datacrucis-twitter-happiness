package com.datacrucis.storm.util;

import java.io.Serializable;
import java.util.Map;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;


public final class MultiCounter implements Serializable {
    private Map<String, AtomicInteger> countsByName = new HashMap<String, AtomicInteger>();

    /**
     * Increment counter by name.
     * If counter with the requested name dosn't exists, create it, 
     * set value to 0 and then increment it.
     */
    public void incrementAndAddIfMissing(final String name) {
        if(!countsByName.containsKey(name)) {
            countsByName.put(name, new AtomicInteger(0));
        }
        countsByName.get(name).getAndIncrement();
    }

    /**
     * Reset counter by name.
     * If counter with the requested name dosn't exists, noop.
     */
    public void resetIfExists(final String name) {
        if (countsByName.containsKey(name)) {
            countsByName.get(name).set(0);
        }
    }

    /**
     * Reset all existing counters.
     */
    public void resetAll() {
        for(String name : countsByName.keySet()) {
            countsByName.get(name).set(0);
        }
    }

    /**
     * Return all existing counters as dict with int values.
     */
    public Map<String, Integer> getAll() {
        if (countsByName.isEmpty()) {
            return null;
        }

        Map<String, Integer> counts = new HashMap<String, Integer>();
        for (Map.Entry<String, AtomicInteger> count : countsByName.entrySet()) {
            counts.put(count.getKey(), count.getValue().intValue());
        }
        return counts;
    }

    /**
     * Return counter's value by name.
     * If counter with the requested name dosn't exists, return 0. 
     */
    public Integer getValueOrZeroIfMissing(final String name) {
        return countsByName.containsKey(name) ? countsByName.get(name).intValue() : 0;
    }
}
