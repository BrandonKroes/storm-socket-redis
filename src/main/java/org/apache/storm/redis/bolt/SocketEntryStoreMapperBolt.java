package org.apache.storm.redis.bolt;

import java.sql.Timestamp;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.redis.common.config.JedisPoolConfig;
import org.apache.storm.redis.common.mapper.RedisDataTypeDescription;
import org.apache.storm.redis.common.mapper.RedisStoreMapper;
import org.apache.storm.redis.spout.SocketSpout;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.ITuple;



public class SocketEntryStoreMapperBolt implements RedisStoreMapper {
    private final RedisDataTypeDescription description;

    public SocketEntryStoreMapperBolt() {
        String hashKey = "SocketToRedisEntry";
        description = new RedisDataTypeDescription(
                RedisDataTypeDescription.RedisDataType.HASH, hashKey);
    }

    @Override
    public RedisDataTypeDescription getDataTypeDescription() {
        return description;
    }

    @Override
    public String getKeyFromTuple(ITuple tuple) {
        return tuple.getStringByField("ID");
    }

    @Override
    public String getValueFromTuple(ITuple tuple) {
        StringBuilder sb = new StringBuilder();
        int size = tuple.getFields().size();
        for (int i = 1; i < size; i++) {
            sb.append(tuple.getValue(i));
        }
        Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        sb.append(" ST" + timestamp.toString());
        return sb.toString();
    }
}
