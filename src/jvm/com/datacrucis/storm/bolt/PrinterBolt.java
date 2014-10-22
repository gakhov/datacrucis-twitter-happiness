package com.datacrucis.storm.bolt;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

import twitter4j.Status;


public class PrinterBolt extends BaseBasicBolt {

    private OutputCollector _collector;

    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this._collector = collector;
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        final Status status = (Status) tuple.getValueByField("status");
        final Integer sentiment = (Integer) tuple.getValueByField("sentiment");
        System.out.println(status.getText());
        System.out.println(sentiment);

        _collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }
}
