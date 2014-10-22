package com.datacrucis.storm.bolt;

import java.util.Map;
import java.util.Properties;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.tuple.Tuple;

import twitter4j.Status;

import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations;
import edu.stanford.nlp.trees.Tree;
import edu.stanford.nlp.util.CoreMap;


public class SentimenterBolt extends BaseBasicBolt {

    private OutputCollector _collector;
    private StanfordCoreNLP _pipelineNLP;

    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this._collector = collector;

        Properties properties = new Properties();
        properties.put("annotators", "tokenize, ssplit, parse, sentiment");
        this._pipelineNLP = new StanfordCoreNLP(properties);
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        final Status status = (Status) tuple.getValueByField("status");

        Integer sentiment = this.getSentiment(status.getText());

        _collector.emit(tuple, new Values(status, sentiment));
        _collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("status", "sentiment"));
    }

    private Integer getSentiment(String tweet) {
        if (tweet == null || tweet.length() < 1) {
            return null;
        }

        Integer sentiment = 0;
        int longestChunk = 0;
        Annotation annotation = _pipelineNLP.process(tweet);
        for (CoreMap sentence : annotation.get(CoreAnnotations.SentencesAnnotation.class)) {
            Tree tree = sentence.get(SentimentCoreAnnotations.AnnotatedTree.class);
            int sentimentClass = RNNCoreAnnotations.getPredictedClass(tree);
            String chunk = sentence.toString();
            if (chunk.length() > longestChunk) {
                sentiment = sentimentClass;
                longestChunk = chunk.length();
            }
        }
        return sentiment;
    }
}
