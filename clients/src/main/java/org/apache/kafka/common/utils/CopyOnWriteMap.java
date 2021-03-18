/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.kafka.common.utils;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

/**
 * A simple read-optimized map implementation that synchronizes only writes and does a full copy on each modification
 *
 * TODO
 *  1) 这个数据结构在高并发的情况下是线程安全的.
 *  2) 采用的是读写分离的设计思想
 *      每次插入(写数据)数据的时候都开辟新的内存空间,写数据会加锁(put)
 *      所以会有这个小缺点,就是插入数据的时候,会比较耗费内存空间
 *  3) 这样的一个数据结构,最适合的场景就是读多写少的场景,这时候读数据的性能很高.
 *
 *
 *  我们目前的场景绝对是读多写少的场景,分析如下:
 *    假设我们有100个partition,那么也只需要往这个batches数据结构里面写入100次的KV值(TopicPartition,Deque<RecordBatch>),即调用100次put方法,
 *    但是如果我们有1亿条数据,那么它需要调用batches数据结构中一亿次的get方法获取Value值Deque,然后将数据写入队列里的RecordBatch批次里
 *    很明显这是读多写少的场景.
 */
public class CopyOnWriteMap<K, V> implements ConcurrentMap<K, V> {

    /**
     * TODO 核心变量就是一个 Map
     *  这个Map有个特点,它的修饰符是 volatile关键字
     *  在多线程的情况下,如果这个 Map的值发生变化,其它线程也是可见的,保证可见性!!!
     *  最重要的是 get和 put方法
     */
    private volatile Map<K, V> map;


    public CopyOnWriteMap() {
        this.map = Collections.emptyMap();
    }

    public CopyOnWriteMap(Map<K, V> map) {
        this.map = Collections.unmodifiableMap(map);
    }

    @Override
    public boolean containsKey(Object k) {
        return map.containsKey(k);
    }

    @Override
    public boolean containsValue(Object v) {
        return map.containsValue(v);
    }

    @Override
    public Set<java.util.Map.Entry<K, V>> entrySet() {
        return map.entrySet();
    }

    /**
     * TODO
     *  没有加锁,读取数据的时候性能很高（高并发的场景下,肯定性能很高）,并且是线程安全的
     *  因为人家采用的是读写分离的思想,这个在 put方法中可以看到,每次put时候是新开辟内存空间,而读的时候是读老的内存空间
     * @param k
     * @return
     */
    @Override
    public V get(Object k) {
        return map.get(k);
    }

    @Override
    public boolean isEmpty() {
        return map.isEmpty();
    }

    @Override
    public Set<K> keySet() {
        return map.keySet();
    }

    @Override
    public int size() {
        return map.size();
    }

    @Override
    public Collection<V> values() {
        return map.values();
    }

    @Override
    public synchronized void clear() {
        this.map = Collections.emptyMap();
    }

    //TODO 非常重要!!!

    /**
     * 1) 整个方法使用的是synchronized关键字去修饰的,说明这个方法是线程安全的.
     *    即使加了锁,这段代码的性能依然很好,因为里面都是纯内存操作
     *
     * 2) 这种设计方式,采用的是读写分离的设计思想
     *    读操作和写操作是相互不影响的
     *    所以我们读数据的操作就是线程安全的.
     *
     * 3) 最后把值赋值给了map,map是用volatile关键字修饰的
     *    说明这个map具有可见性,这样的话,如果get数据的时候,这儿的值发生了变化,也是能感知到的
     *
     *
     * 这个 CopyOnWriteMap数据结构适合什么样的一个场景呢？
     *  读多写少!!!
     *
     * @param k
     * @param v
     * @return
     */
    @Override
    public synchronized V put(K k, V v) {
        //每次写入数据,都会构建新的Map内存空间
        //采用了读写分离的思想
        //往新的内存空间里面插入数据
        //读,读数据就读老的内存空间
        Map<K, V> copy = new HashMap<K, V>(this.map);
        //插入数据
        V prev = copy.put(k, v);
        //赋值给map,由于map是volatile修饰的,能保证这一步map改变后其它线程的可见性
        //TODO
        // 关于Collections.unmodifiableMap(copy):
        //  该方法返回了一个map的不可修改的视图umap,
        //  为用户提供了一种生成只读容器的方法。如果尝试修改该容器umap, 将会抛出UnsupportedOperationException异常。
        this.map = Collections.unmodifiableMap(copy);
        return prev;
    }

    @Override
    public synchronized void putAll(Map<? extends K, ? extends V> entries) {
        Map<K, V> copy = new HashMap<K, V>(this.map);
        copy.putAll(entries);
        this.map = Collections.unmodifiableMap(copy);
    }

    @Override
    public synchronized V remove(Object key) {
        Map<K, V> copy = new HashMap<K, V>(this.map);
        V prev = copy.remove(key);
        this.map = Collections.unmodifiableMap(copy);
        return prev;
    }

    @Override
    public synchronized V putIfAbsent(K k, V v) {
        if (!containsKey(k))
            return put(k, v);
        else
            return get(k);
    }

    @Override
    public synchronized boolean remove(Object k, Object v) {
        if (containsKey(k) && get(k).equals(v)) {
            remove(k);
            return true;
        } else {
            return false;
        }
    }

    @Override
    public synchronized boolean replace(K k, V original, V replacement) {
        if (containsKey(k) && get(k).equals(original)) {
            put(k, replacement);
            return true;
        } else {
            return false;
        }
    }

    @Override
    public synchronized V replace(K k, V v) {
        if (containsKey(k)) {
            return put(k, v);
        } else {
            return null;
        }
    }

}
