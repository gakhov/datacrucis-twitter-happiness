package com.datacrucis.storm;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.log4j.Logger;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;

import com.datacrucis.storm.bolt.CounterBolt;
import com.datacrucis.storm.bolt.ESWriterBolt;
import com.datacrucis.storm.bolt.MoodEstimatorBolt;
import com.datacrucis.storm.bolt.SentimenterBolt;
import com.datacrucis.storm.spout.TwitterStreamSpout;


class TwitterHappinessTopology {

    public static Logger LOG = Logger.getLogger(TwitterHappinessTopology.class);

    public static void main(String[] args) throws Exception {
        String topologyName = args[0];
        String propertiesPath = args[1];

        PropertiesConfiguration properties = new PropertiesConfiguration(propertiesPath);
        boolean isLocalMode = properties.getBoolean("storm.local_mode");

        String  esClusterName = properties.getString("es.cluster_name");
        String  esHost        = properties.getString("es.host");
        Integer esPort        = properties.getInt("es.port");
        String  esItemType    = properties.getString("es.item_type");
        boolean esLocalMode   = properties.getBoolean("es.local_mode");

        String esIndexNameForMood   = properties.getString("es.index.name_for_mood");
        String esIndexNameForCounts = properties.getString("es.index.name_for_counts");

        String twitterConsumerKey       = properties.getString("twitter.consumer.key");
        String twitterConsumerSecret    = properties.getString("twitter.consumer.secret");
        String twitterAccessTokenKey    = properties.getString("twitter.access.key");
        String twitterAccessTokenSecret = properties.getString("twitter.access.secret");

        String[] twitterQueryKeywords   = properties.getStringArray("twitter.query.keywords");
        String[] twitterQueryLanguage   = {"en"};

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
            "spout",
            new TwitterStreamSpout(
                twitterConsumerKey,
                twitterConsumerSecret,
                twitterAccessTokenKey,
                twitterAccessTokenSecret,
                twitterQueryKeywords,
                null,
                twitterQueryLanguage)
        );

        builder.setBolt(
            "sentiment",
            new SentimenterBolt()
        ).shuffleGrouping("spout");

        builder.setBolt(
            "counter",
            new CounterBolt(60)
        ).shuffleGrouping("sentiment");

        builder.setBolt(
            "es_counts",
            new ESWriterBolt(esClusterName, esHost, esPort, esIndexNameForCounts, esLocalMode)
        ).shuffleGrouping("counter", "counts");

        builder.setBolt(
            "mood",
            new MoodEstimatorBolt()
        ).shuffleGrouping("counter", "mood");

        builder.setBolt(
            "es_mood",
            new ESWriterBolt(esClusterName, esHost, esPort, esIndexNameForMood, esLocalMode)
        ).shuffleGrouping("mood");

        if (!isLocalMode) {
            StormSubmitter.submitTopologyWithProgressBar(
                topologyName, conf, builder.createTopology());
        }
        else {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology(topologyName, conf, builder.createTopology());
            Thread.sleep(1000000);
            cluster.killTopology(topologyName);
            cluster.shutdown();
        }
    }
}
