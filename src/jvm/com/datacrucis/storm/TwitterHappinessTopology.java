package com.datacrucis.storm;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.log4j.Logger;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;

import com.datacrucis.storm.bolt.PrinterBolt;
import com.datacrucis.storm.spout.TwitterStreamSpout;


class TwitterHappinessTopology {

    public static Logger LOG = Logger.getLogger(TwitterHappinessTopology.class);

    public static void main(String[] args) throws Exception {
        String topologyName = args[0];
        String propertiesPath = args[1];

        PropertiesConfiguration properties = new PropertiesConfiguration(propertiesPath);
        boolean isLocalMode = properties.getBoolean("storm.local_mode");

        String twitterConsumerKey       = properties.getString("twitter.consumer.key");
        String twitterConsumerSecret    = properties.getString("twitter.consumer.secret");
        String twitterAccessTokenKey    = properties.getString("twitter.access.key");
        String twitterAccessTokenSecret = properties.getString("twitter.access.secret");

        String[] twitterQueryKeywords   = properties.getStringArray("twitter.query.keywords");
        String[] twitterQueryLanguage   = properties.getStringArray("twitter.query.language");

        final Config conf = new Config();
        conf.setNumWorkers(
            properties.getInt("storm.num_workers"));
        conf.setMaxSpoutPending(
            properties.getInt("storm.max_spout_pending"));
        conf.setMaxTaskParallelism(
            properties.getInt("storm.max_task_parallel"));
        conf.setMessageTimeoutSecs(
            properties.getInt("storm.message_timeout_secs"));

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout(
            "spout", new TwitterStreamSpout(
                twitterConsumerKey,
                twitterConsumerSecret,
                twitterAccessTokenKey,
                twitterAccessTokenSecret,
                twitterQueryKeywords,
                null,
                twitterQueryLanguage)
        );
        builder.setBolt(
            "print", new PrinterBolt()
        ).shuffleGrouping("spout");

        if (!isLocalMode) {
            StormSubmitter.submitTopologyWithProgressBar(
                topologyName, conf, builder.createTopology());
        }
        else {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology(topologyName, conf, builder.createTopology());
            Thread.sleep(10000);
            cluster.killTopology(topologyName);
            cluster.shutdown();
        }
    }
}
