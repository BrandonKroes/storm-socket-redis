package org.apache.storm.redis.bolt;

import java.sql.Timestamp;
import java.util.Map;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

public class SplitSocketEntryBolt extends BaseRichBolt {
    private OutputCollector collector;

    public void prepare(Map config, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    public void execute(Tuple tuple) {
        Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        String sentence = tuple.getStringByField("socketEntry");
        String[] records = sentence.split(",");
        this.collector.emit(new Values(records[0], records[1], records[2], " PR" + timestamp.toString()));
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("ID", "GenerationTime", "ArrivalTime", "ProcessingTime"));
    }
}