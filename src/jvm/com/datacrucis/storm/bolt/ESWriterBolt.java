package com.datacrucis.storm.bolt;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import java.util.TimeZone;

import java.text.DateFormat;
import java.text.SimpleDateFormat;

import org.apache.log4j.Logger;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

import static org.elasticsearch.node.NodeBuilder.*;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;


public class ESWriterBolt extends BaseRichBolt{

    private OutputCollector collector;
    private Client client;

    private String esClusterName;
    private String esHost;
    private Integer esPort;
    private String indexName;
    private Boolean isLocalMode;

    private static DateFormat esISOFormatter;

    private static final Logger log = Logger.getLogger(ESWriterBolt.class);

    public ESWriterBolt(String esClusterName, String esHost, Integer esPort,
                        String indexName, Boolean isLocalMode) {
        super();
        this.esClusterName = esClusterName;
        this.esHost = esHost;
        this.esPort = esPort;
        this.indexName = indexName;
        this.isLocalMode = isLocalMode;

        TimeZone utc = TimeZone.getTimeZone("UTC");
        this.esISOFormatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'");
        this.esISOFormatter.setTimeZone(utc);
    }

    @SuppressWarnings("rawtypes")
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;

        if (isLocalMode) {
            log.info("local ES node");

            Settings settings = ImmutableSettings.settingsBuilder()
                .put("action.auto_create_index", true)
                .build();
            client = nodeBuilder().settings(settings)
                .data(true)
                .client(false)
                .local(true)
                .build()
                .start()
                .client();
        }
        else {
            log.info("joining the ES cluster: " + esClusterName);

            Settings settings = ImmutableSettings.settingsBuilder()
                .put("cluster.name", esClusterName)
                .build();
            client = new TransportClient(settings).addTransportAddress(
                new InetSocketTransportAddress(esHost, esPort));
        }

    }

    private String getESTimestamp(final Date date) {
        return esISOFormatter.format(date);
    }

    private Map<String, Object> getESItem(final Map<String, Object> item, final Date date) {
        Map<String, Object> esItem = new HashMap<String, Object>();
        esItem.putAll(item);
        esItem.put("timestamp", getESTimestamp(date));
        return esItem;
    }

    @Override
    public void execute(Tuple tuple) {
        final Map<String, Object> item = (HashMap<String, Object>) tuple.getValueByField("item");
        final String itemType = (String) tuple.getValueByField("period");
        final Date date = (Date) tuple.getValueByField("date");

        Map<String, Object> esItem = getESItem(item, date);
        // try {
            IndexResponse response = client.prepareIndex(indexName, itemType)
                .setSource(esItem)
                .execute()
                .actionGet();
        // } catch (Exception e) {
        //     collector.fail(tuple);
        //     log.warn("Index request failed ...");
        //     return;
        // }

        collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer){
        // doesn't return any data
    }
}