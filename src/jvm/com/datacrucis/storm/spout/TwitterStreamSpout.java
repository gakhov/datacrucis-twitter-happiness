/*
Inspired by TwitterSampleSpout.java from storm-starter
*/
package com.datacrucis.storm.spout;

import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import twitter4j.FilterQuery;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.auth.AccessToken;
import twitter4j.conf.ConfigurationBuilder;

import backtype.storm.Config;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;


@SuppressWarnings("serial")
public class TwitterStreamSpout extends BaseRichSpout {

    SpoutOutputCollector _collector;
    LinkedBlockingQueue<Status> queue = null;
    TwitterStream _twitterStream;

    /* Twitter API consumer key */
    String consumerKey;

    /* Twitter API consumer secret */
    String consumerSecret;

    /* Twitter API access token */
    String accessToken;

    /* Twitter API access token secret */
    String accessTokenSecret;

    /* keywords to track */
    String[] keywords;

    /* Tweets language of the stream */
    String[] language;

    public TwitterStreamSpout(String consumerKey, String consumerSecret,
            String accessToken, String accessTokenSecret, String[] keywords,
            String[] language) {
        this.consumerKey = consumerKey;
        this.consumerSecret = consumerSecret;
        this.accessToken = accessToken;
        this.accessTokenSecret = accessTokenSecret;
        this.keywords = keywords;
        this.language = language;
    }

    public TwitterStreamSpout() {
    }

    @Override
    public void open(Map conf, TopologyContext context,
            SpoutOutputCollector collector) {
        queue = new LinkedBlockingQueue<Status>(1000);
        _collector = collector;

        StatusListener listener = new StatusListener() {

            @Override
            public void onStatus(Status status) {
                queue.offer(status);
            }

            @Override
            public void onDeletionNotice(StatusDeletionNotice sdn) {
            }

            @Override
            public void onTrackLimitationNotice(int i) {
            }

            @Override
            public void onScrubGeo(long l, long l1) {
            }

            @Override
            public void onException(Exception ex) {
            }

            @Override
            public void onStallWarning(StallWarning arg0) {
            }

        };

        TwitterStream twitterStream = new TwitterStreamFactory(
            new ConfigurationBuilder().setJSONStoreEnabled(true).build())
            .getInstance();

        twitterStream.addListener(listener);
        twitterStream.setOAuthConsumer(consumerKey, consumerSecret);
        AccessToken token = new AccessToken(accessToken, accessTokenSecret);
        twitterStream.setOAuthAccessToken(token);

        // TODO: this is not enough, since it could be list with empty string
        if (keywords == null) {
            twitterStream.sample();
        }
        else {
            FilterQuery query = new FilterQuery();

            query.track(keywords);
            if (language != null && language.length > 0) {
                query.language(language);
            }

            twitterStream.filter(query);
        }
    }

    @Override
    public void nextTuple() {
        Status ret = queue.poll();
        if (ret == null) {
            Utils.sleep(50);
        } else {
            _collector.emit(new Values(ret));
        }
    }

    @Override
    public void close() {
        _twitterStream.shutdown();
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Config ret = new Config();
        ret.setMaxTaskParallelism(1);
        return ret;
    }

    @Override
    public void ack(Object id) {
    }

    @Override
    public void fail(Object id) {
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("status"));
    }
}
