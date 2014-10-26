package com.datacrucis.storm.util;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.HashSet;

import com.datacrucis.storm.util.MultiCounter;

/**
 * Special type of Counter that allows special names only.
 */
public final class SentimentCounter implements Serializable {

    private static final Set<String> SUPPORTED_NAMES = new HashSet<String>(
        Arrays.asList("VeryNegative", "Negative", "Neutral", "Positive", "VeryPositive")
    );

    private MultiCounter sentimentCounter = new MultiCounter();

    private boolean isValidName(String name) {
        return SUPPORTED_NAMES.contains(name);
    }

    public void increment(String name) {
        if(!isValidName(name)) {
            throw new IllegalArgumentException("Unexpected name for the sentiment key");
        }
        sentimentCounter.incrementAndAddIfMissing(name);
    }

    public Integer get(String name) {
        return sentimentCounter.getValueOrZeroIfMissing(name);
    }

    public Map<String, Integer> getAll() {
        return sentimentCounter.getAll();
    }

    public void reset() {
        sentimentCounter.resetAll();
    }
}
