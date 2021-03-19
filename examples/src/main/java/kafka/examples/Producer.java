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
package kafka.examples;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class Producer extends Thread {
    private final KafkaProducer<Integer, String> producer;
    private final String topic;
    private final Boolean isAsync;

    public Producer(String topic, Boolean isAsync) {
        Properties props = new Properties();
        //用于拉取Kafka的元数据,每个broker的元数据是一致且完整的,但是一般写三个,防止Broker挂掉
        props.put("bootstrap.servers", "localhost:9092");
        props.put("client.id", "DemoProducer");

        //设置K、V的序列化类,因为网路传输是二进制的格式
        props.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        //相反,消费者消费的时候就需要反序列化
        //TODO 初始化KafkaProducer
        producer = new KafkaProducer<>(props);
        this.topic = topic;
        this.isAsync = isAsync;
    }

    public void run() {
        int messageNo = 1;
        //循环一直往Kafka发送数据
        while (true) {
            String messageStr = "Message_" + messageNo;
            long startTime = System.currentTimeMillis();
            //isAsync kafka发送数据的时候,有两种方式
            //1: 异步发送
            //2: 同步发送
            //isAsync: true的时候是异步发送; false就是同步发送
            if (isAsync) { // Send asynchronously
                //TODO 异步发送
                // 一直发送消息, 消息响应的结果交给回调函数处理
                // 这样性能比较好(生产使用)
                producer.send(new ProducerRecord<>(topic,
                    messageNo,
                    messageStr), new DemoCallBack(startTime, messageNo, messageStr));
            } else { // Send synchronously
                try {
                    //TODO 同步发送
                    // 发送一条消息,等这条消息所有的后续工作都完成了以后才继续发送下一条消息
                    producer.send(new ProducerRecord<>(topic,
                        messageNo,
                        messageStr)).get();
                    System.out.println("Sent message: (" + messageNo + ", " + messageStr + ")");
                } catch (InterruptedException | ExecutionException e) {
                    e.printStackTrace();
                }
            }
            ++messageNo;
        }
    }
}

class DemoCallBack implements Callback {

    private final long startTime;
    private final int key;
    private final String message;

    public DemoCallBack(long startTime, int key, String message) {
        this.startTime = startTime;
        this.key = key;
        this.message = message;
    }

    /**
     * A callback method the user can implement to provide asynchronous handling of request completion. This method will
     * be called when the record sent to the server has been acknowledged. Exactly one of the arguments will be
     * non-null.
     *
     * @param metadata  The metadata for the record that was sent (i.e. the partition and offset). Null if an error
     *                  occurred.
     * @param exception The exception thrown during processing of this record. Null if no error occurred.
     */
    public void onCompletion(RecordMetadata metadata, Exception exception) {
        //回调函数执行onCompletion方法,根据自己的逻辑处理
        long elapsedTime = System.currentTimeMillis() - startTime;
        if (metadata != null) {
            System.out.println(
                "message(" + key + ", " + message + ") sent to partition(" + metadata.partition() +
                    "), " +
                    "offset(" + metadata.offset() + ") in " + elapsedTime + " ms");
        } else {
            //此时说明发送有异常,一般在生产里面,还会有其他备用链路来保证数据不丢
            exception.printStackTrace();
        }
    }
}
