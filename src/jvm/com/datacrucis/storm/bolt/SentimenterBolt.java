package com.datacrucis.storm.bolt;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.log4j.Logger;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
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


public class SentimenterBolt extends BaseRichBolt {

    private OutputCollector collector;
    private StanfordCoreNLP pipelineNLP;

    private static final Logger log = Logger.getLogger(SentimenterBolt.class);

    @SuppressWarnings("rawtypes")
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;

        Properties properties = new Properties();
        properties.setProperty("annotators", "tokenize, ssplit, parse, sentiment");
        pipelineNLP = new StanfordCoreNLP(properties);
    }

    @Override
    public void execute(Tuple tuple) {
        final Status status = (Status) tuple.getValueByField("status");

        Integer sentiment = this.getSentiment(status.getText());

        collector.emit(tuple, new Values(status, sentiment));
        collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("status", "sentiment"));
    }

    /**
     * Return a sentiment value of the input string.
     *
     * If input string empty or null, this method returns null, otherwise
     * 0 - Very Negative
     * 1 - Negative
     * 2 - Neutral
     * 3 - Positive
     * 4 - Very Positive
     *
     * @param  tweet  string content of the Twitter status
     * @return        sentiment value [0, 4]
     */
    private Integer getSentiment(String tweet) {
        if (tweet == null || tweet.length() < 1) {
            log.warn("Empty tweet detected, skipping ...");
            return null;
        }

        // avoid badly encoded emoticons
        tweet = tweet.replaceAll("[^\\x00-\\x7f-\\x80-\\xad]", "");

        Integer sentiment = 0;
        int longestChunk = 0;

        Annotation annotation = pipelineNLP.process(tweet);
        for (CoreMap sentence : annotation.get(CoreAnnotations.SentencesAnnotation.class)) {
            Tree tree = sentence.get(SentimentCoreAnnotations.AnnotatedTree.class);
            Integer sentimentClass = RNNCoreAnnotations.getPredictedClass(tree);
            String chunk = sentence.toString();
            if (chunk.length() > longestChunk) {
                sentiment = sentimentClass;
                longestChunk = chunk.length();
            }
        }
        return sentiment;
    }
}
