package org.apache.storm.redis;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.redis.bolt.RedisStoreBolt;
import org.apache.storm.redis.bolt.SocketEntryStoreMapperBolt;
import org.apache.storm.redis.bolt.SplitSocketEntryBolt;
import org.apache.storm.redis.common.config.JedisPoolConfig;
import org.apache.storm.redis.common.mapper.RedisDataTypeDescription;
import org.apache.storm.redis.common.mapper.RedisStoreMapper;
import org.apache.storm.redis.spout.SocketSpout;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.ITuple;

public class SocketToRedis {
    
    private static final String WORD_SPOUT = "SOCKET_SPOUT";
    private static final String COUNT_BOLT = "SPLIT_BOLT";
    private static final String STORE_BOLT = "STORE_BOLT";

    private static final String REDIS_HOST = "127.0.0.1";
    private static final int REDIS_PORT = 6379;
    private static final String SOCKET_HOST = "127.0.0.1";
    private static final int SOCKET_PORT = 9999;

    public static void main(String[] args) throws Exception {
        String redisHost = REDIS_HOST;
        int redisPort = REDIS_PORT;
        
        String socketHost = SOCKET_HOST;
        int socketPort = SOCKET_PORT;

        if (args.length >= 2) {
            redisHost = args[0];
            redisPort = Integer.parseInt(args[1]);
        }

        if (args.length >= 4) {
            socketHost = args[2];
            socketPort = Integer.parseInt(args[3]);
        }

        JedisPoolConfig poolConfig = new JedisPoolConfig.Builder().setHost(redisHost).setPort(redisPort).build();

        SocketSpout spout = new SocketSpout(socketHost, socketPort);
        SplitSocketEntryBolt bolt = new SplitSocketEntryBolt();
        RedisStoreMapper storeMapper = new SocketEntryStoreMapperBolt();
        RedisStoreBolt store = new RedisStoreBolt(poolConfig, storeMapper);

        // SocketSpout ==> countBolt ==> RedisBolt
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout(WORD_SPOUT, spout, 1);
        builder.setBolt(COUNT_BOLT, bolt, 1).fieldsGrouping(WORD_SPOUT, new Fields("socketEntry"));
        builder.setBolt(STORE_BOLT, store, 1).shuffleGrouping(COUNT_BOLT);

        String topoName = "test";
        if (args.length == 5) {
            topoName = args[4];
        } else if (args.length > 5) {
            System.out.println("Usage: SocketToRedis: <redis host> <redis port> <socket host> <socket port> (topology name)");
            return;
        }
        Config config = new Config();
        StormSubmitter.submitTopology(topoName, config, builder.createTopology());
    }
}


