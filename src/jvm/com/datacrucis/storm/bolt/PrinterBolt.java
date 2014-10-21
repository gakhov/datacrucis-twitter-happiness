package com.datacrucis.storm.bolt;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

import twitter4j.Status;


public class PrinterBolt extends BaseBasicBolt {

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        final Status status = (Status) input.getValueByField("tweet");
        final int sentiment = (Status) input.getValueByField("sentiment");
        System.out.println(status.getText());
        System.out.println(sentiment);

        collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }
}
