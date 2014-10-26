package com.datacrucis.storm.bolt;

import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import org.apache.log4j.Logger;

import backtype.storm.Config;
import backtype.storm.Constants;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import com.datacrucis.storm.util.SentimentPeriodCounter;


public class CounterBolt extends BaseRichBolt {

    private OutputCollector collector;

    private SentimentPeriodCounter sentimentPeriodCounter = new SentimentPeriodCounter(
        new HashSet<String>(
            Arrays.asList("shortTerm", "middleTerm", "longTerm"))
    );
    private final String[] sentimentToClass = {
        "VeryNegative", 
        "Negative",
        "Neutral",
        "Positive",
        "VeryPositive"
    };
    
    private int tickTupleCount;
    private int tickFrequencyInSeconds;

    private static final Logger log = Logger.getLogger(CounterBolt.class);

    public CounterBolt(int tickFrequencyInSeconds) {
        this.tickTupleCount = 0;
        this.tickFrequencyInSeconds = tickFrequencyInSeconds;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    private boolean isTickTuple(Tuple tuple) {
        return tuple.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID) 
            && tuple.getSourceStreamId().equals(Constants.SYSTEM_TICK_STREAM_ID);
    }

    private boolean isLongTermTickTuple() {
        int ticksFor30Minutes = Math.round(1800 / tickFrequencyInSeconds);
        return tickTupleCount > 0 && tickTupleCount % ticksFor30Minutes == 0;
    }

    @Override
    public void execute(Tuple tuple) {
        if (isTickTuple(tuple)) {
            Date now = new Date();
            if (isLongTermTickTuple()) {
                Map<String, Integer> longTermCounts = sentimentPeriodCounter.getAllForPeriod("longTerm");
                sentimentPeriodCounter.reset("longTerm");
                collector.emit("counts", new Values(longTermCounts, "long", now));
                collector.emit("mood", new Values(longTermCounts, "long", now));
                log.info("Emiting longTerm counts ...");
            }

            Map<String, Integer> shortTermCounts = sentimentPeriodCounter.getAllForPeriod("shortTerm");
            sentimentPeriodCounter.reset("shortTerm");
            collector.emit("counts", new Values(shortTermCounts, "short", now));
            collector.emit("mood", new Values(shortTermCounts, "short", now));
            log.info("Emiting shortTerm counts ...");

            tickTupleCount++;
            collector.ack(tuple);
            return;
        }

        final Integer sentiment = (Integer) tuple.getValueByField("sentiment");
        if (sentiment == null || sentiment < 0 || sentiment > sentimentToClass.length) {
            // undefined sentiment, so just ACK the tuple without any action
            log.warn("No sentiment detected, skipping ...");
            collector.ack(tuple);
            return;
        }

        String sentimentClass = sentimentToClass[sentiment];
        sentimentPeriodCounter.increment(sentimentClass);
        collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(final OutputFieldsDeclarer declarer) {
        declarer.declareStream("counts", new Fields("item", "period", "date"));
        declarer.declareStream("mood", new Fields("counts", "period", "date"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Config conf = new Config();
        conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, tickFrequencyInSeconds);
        return conf;
    }

}
