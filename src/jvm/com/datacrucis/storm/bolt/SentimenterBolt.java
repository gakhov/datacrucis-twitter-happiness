package com.datacrucis.storm.bolt;

import java.util.Properties;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
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

    private OutputCollector collector;
    private StanfordCoreNLP pipelineNLP;

    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;

        Properties properties = new Properties();
        properties.put("annotators", "tokenize, ssplit, parse, sentiment")
        this.pipelineNLP = new StanfordCoreNLP(properties);
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        final Status status = (Status) input.getValueByField("tweet");

        int sentiment = this.getSentiment(status.getText());

        collector.emit(tuple, new Values(tweet, sentiment)));
        collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("tweet", "sentiment"));
    }

    private int getSentiment(String tweet) {
        if (tweet == null || tweet.length() < 1) {
            return null;
        }

        int sentiment = 0;
        int longestChunk = 0;
        Annotation annotation = pipelineNLP.process(tweet);
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
