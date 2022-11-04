/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package org.apache.storm.redis.spout;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Socket;
import java.sql.Timestamp;
import java.util.Map;
import java.util.Random;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SocketSpout extends BaseRichSpout {
    SpoutOutputCollector collector;
    private TopologyContext context;

    private BufferedReader in;

    private final String host;
    private final int port;

    public SocketSpout(String host, int port) throws IOException {
        this.host = host;
        this.port = port;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("socketEntry"));
    }

    @Override
    public void open(Map config, TopologyContext context,
                     SpoutOutputCollector collector) {
        this.collector = collector;
        try {
            Socket s = new Socket(host, port);
            in = new BufferedReader(
                    new InputStreamReader(s.getInputStream()));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void nextTuple() {
        try {
            String rawTuple = in.readLine();

            if (rawTuple == null) {
                throw new SocketSpoutRuntimeException();
            }

            Timestamp timestamp = new Timestamp(System.currentTimeMillis());
            this.collector.emit(new Values(rawTuple + ", AT" + timestamp.toString()));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static class SocketSpoutRuntimeException extends RuntimeException {
    }


}
