package com.datacrucis.storm.bolt;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.tuple.Tuple;


public class MoodEstimatorBolt extends BaseRichBolt {

    private OutputCollector collector;

    private static final Logger log = Logger.getLogger(MoodEstimatorBolt.class);

    @SuppressWarnings("rawtypes")
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple tuple) {
        final Map<String, Integer> counts = (HashMap<String, Integer>) tuple.getValueByField("counts");
        final String period = (String) tuple.getValueByField("period");
        final Date date = (Date) tuple.getValueByField("date");

        Map<String, Object> mood = this.calculateMood(counts);

        collector.emit(tuple, new Values(mood, period, date));
        collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("item", "period", "date"));
    }

    /**
     * Calculate mood value based on period counts.
     * The algorithm is quite simple, just calculate mood by formula
     * (-2 * VeryNeg - Neg + Pos + 2 * VeryPos) / total
     *
     * If counts empty or null, this method returns null, otherwise dict with values
     *
     * @param  counts  dict with sentiment values
     * @return         mood dict
     */
    private Map<String, Object> calculateMood(Map<String, Integer> counts) {
        if(counts.isEmpty()) {
            return null;
        }

        float moodValue = 0;
        int total = 0;
        if(counts.containsKey("VeryNegative")) {
            moodValue -= 2 * counts.get("VeryNegative");
            total += counts.get("VeryNegative");
        }
        if(counts.containsKey("Negative")) {
            moodValue -= counts.get("Negative");
            total += counts.get("Negative");
        }
        if(counts.containsKey("Neutral")) {
            total += counts.get("Neutral");
        }
        if(counts.containsKey("Positive")) {
            moodValue += counts.get("Positive");
            total += counts.get("Positive");
        }
        if(counts.containsKey("VeryPositive")) {
            moodValue += 2 * counts.get("VeryPositive");
            total += counts.get("VeryPositive");
        }

        if(total > 0) {
            moodValue /= total;
        }

        String moodClass;
        if(moodValue > 0.05) {
            moodClass = new String("pos");
        }
        else if(moodValue < -0.05) {
            moodClass = new String("neg");
        }
        else {
            moodClass = new String("neu");
        }

        Map<String, Object> mood = new HashMap<String, Object>();
        mood.put("moodValue", moodValue);
        mood.put("moodClass", moodClass);
        return mood;
    }
}
