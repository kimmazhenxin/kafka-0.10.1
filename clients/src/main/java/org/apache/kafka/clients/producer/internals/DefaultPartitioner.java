/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.clients.producer.internals;

import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.utils.Utils;

/**
 * The default partitioning strategy:
 * <ul>
 * <li>If a partition is specified in the record, use it
 * <li>If no partition is specified but a key is present choose a partition based on a hash of the key
 * <li>If no partition or key is present choose a partition in a round-robin fashion
 */
public class DefaultPartitioner implements Partitioner {

    //原子类,初始化的时候给了一个随机数(JUC 计数器)
    private final AtomicInteger counter = new AtomicInteger(new Random().nextInt());

    public void configure(Map<String, ?> configs) {}

    /**
     * Compute the partition for the given record.
     *
     * @param topic The topic name
     * @param key The key to partition on (or null if no key)
     * @param keyBytes serialized key to partition on (or null if no key)
     * @param value The value to partition on or null
     * @param valueBytes serialized value to partition on or null
     * @param cluster The current cluster metadata
     */
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        //首先获取到我们要发送消息的对应的Topic的分区的信息
        List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
        //计算出来分区的总的个数
        int numPartitions = partitions.size();

        //策略一:如果发送消息的时候,没有指定key,那么消息发送到broker上是轮询策略
        if (keyBytes == null) {
            //这里有一个计数器,每次执行会加一
            //JUC 计数器
            int nextValue = counter.getAndIncrement();
            //获取可用的分区数
            List<PartitionInfo> availablePartitions = cluster.availablePartitionsForTopic(topic);
            // 可用分区数大于0
            if (availablePartitions.size() > 0) {
                //计算要发送到哪个分区上
                //假如一个数如果对10进行取模,结果是0-9之间
                //11 % 10    1
                //12 % 10    2
                //13 % 10    3
                //......
                //实现一个轮询的效果,能达到负载均衡
                int part = Utils.toPositive(nextValue) % availablePartitions.size();
                //根据这个值分配分区号
                return availablePartitions.get(part).partition();
            } else {
                // no partitions are available, give a non-available partition
                return Utils.toPositive(nextValue) % numPartitions;
            }
        } else {
            // hash the keyBytes to choose a partition
            //策略二:如果发送消息的时候,指定key,那么取key的hash值取余分区总数,这样同一个key发送到一个分区上
            return Utils.toPositive(Utils.murmur2(keyBytes)) % numPartitions;
        }
    }

    public void close() {}

}
