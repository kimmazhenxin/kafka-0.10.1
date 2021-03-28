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

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Rate;
import org.apache.kafka.common.utils.Time;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;


/**
 * A pool of ByteBuffers kept under a given memory limit. This class is fairly specific to the needs of the producer. In
 * particular it has the following properties:
 * <ol>
 * <li>There is a special "poolable size" and buffers of this size are kept in a free list and recycled
 * <li>It is fair. That is all memory is given to the longest waiting thread until it has sufficient memory. This
 * prevents starvation or deadlock when a thread asks for a large chunk of memory and needs to block until multiple
 * buffers are deallocated.
 * </ol>
 */
public final class BufferPool {//内存池,缓冲池

    private final long totalMemory;
    private final int poolableSize;
    private final ReentrantLock lock;
    //TODO 内存池就是一个队列,队列里面放的就是一块一块的内存ByteBuffer,和连接池一个道理
    // 可以实现对内存的复用,降低FullGC的概率
    private final Deque<ByteBuffer> free;
    // 存储正在等待分配的内存线程,只要这个有值,就代表此时内存池中的内存分配不够
    private final Deque<Condition> waiters;
    private long availableMemory;
    private final Metrics metrics;
    private final Time time;
    private final Sensor waitTime;

    /**
     * Create a new buffer pool
     * 
     * @param memory The maximum amount of memory that this buffer pool can allocate
     * @param poolableSize The buffer size to cache in the free list rather than deallocating
     * @param metrics instance of Metrics
     * @param time time instance
     * @param metricGrpName logical group name for metrics
     */
    public BufferPool(long memory, int poolableSize, Metrics metrics, Time time, String metricGrpName) {
        this.poolableSize = poolableSize;
        this.lock = new ReentrantLock();
        this.free = new ArrayDeque<ByteBuffer>();
        this.waiters = new ArrayDeque<Condition>();
        this.totalMemory = memory;
        this.availableMemory = memory;
        this.metrics = metrics;
        this.time = time;
        this.waitTime = this.metrics.sensor("bufferpool-wait-time");
        MetricName metricName = metrics.metricName("bufferpool-wait-ratio",
                                                   metricGrpName,
                                                   "The fraction of time an appender waits for space allocation.");
        this.waitTime.add(metricName, new Rate(TimeUnit.NANOSECONDS));
    }

    /**
     * Allocate a buffer of the given size. This method blocks if there is not enough memory and the buffer pool
     * is configured with blocking mode.
     * 
     * @param size The buffer size to allocate in bytes
     * @param maxTimeToBlockMs The maximum time in milliseconds to block for buffer memory to be available
     * @return The buffer
     * @throws InterruptedException If the thread is interrupted while blocked
     * @throws IllegalArgumentException if size is larger than the total memory controlled by the pool (and hence we would block
     *         forever)
     */
    public ByteBuffer allocate(int size, long maxTimeToBlockMs) throws InterruptedException {
        //如果你想要申请的内存的大小,超过了配置的buffer.memory大小(默认32M),那么会抛出异常
        if (size > this.totalMemory)
            throw new IllegalArgumentException("Attempt to allocate " + size
                                               + " bytes, but there is a hard limit of "
                                               + this.totalMemory
                                               + " on memory allocations.");
        //TODO 加锁的代码
        this.lock.lock();
        try {
            // check if we have a free buffer of the right size pooled
            //TODO
            // poolableSize 代表的是一个批次的大小,默认情况下一个批次的大小是16KB
            // 如果我们申请的批次的大小等于设定好的的一个批次的代大小并且内存池不为空,那么直接从内存池里面获取一块内存就可以使用了
            // 和连接池道理一样

            // 场景驱动第一次进来内存池里面是没有内存的,所以这里的获取不到内存
            if (size == poolableSize && !this.free.isEmpty())
                //从队列的头部取出内存块分配,并从队列中移除该内存块
                return this.free.pollFirst();

            // now check if the request is immediately satisfiable with the
            // memory on hand or if we need to block
            // 内存池的个数 * 批次的大小 = free的大小,也即此时内存池内存的大小(刚开始为0,因为个数为0)
            int freeListSize = this.free.size() * this.poolableSize;
            // this.availableMemory + freeListSize: 在第一次执行的刚开始阶段,这两个加起来是32M,其实availableMemory是32M,因为刚开始freeListSize是为0的
            // this.availableMemory + freeListSize: 目前可用的总内存
            // size: 我们这次要申请的内存
            // this.availableMemory + freeListSize >= size: 目前可用的总内存 大于 这次申请的内存,那么就直接分配内存
            // TODO 这个分支执行的是内存够用的情况
            if (this.availableMemory + freeListSize >= size) {
                // we have enough unallocated or pooled memory to immediately
                // satisfy the request
                freeUp(size);
                this.availableMemory -= size;
                lock.unlock();
                return ByteBuffer.allocate(size);// 默认16KB
            } else {
                //we are out of memory and will have to block
                //TODO  这个分支执行的是内存不够用的情况
                // 还有一种情况就是,我们整个内存池还剩10KB的内存,但是我们此次申请的内存时32KB
                // 我们的一条消息,就是32KB -> max(16,32) = 当前批次 = 32KB
                //统计分配的内存
                int accumulated = 0;
                ByteBuffer buffer = null;
                //JUC包,Condition用于线程间的通信
                Condition moreMemory = this.lock.newCondition();
                long remainingTimeToBlockNs = TimeUnit.MILLISECONDS.toNanos(maxTimeToBlockMs);
                //等待别人释放内存.只要这个有值,说明此时内存池内存不够,线程只能等待分配内存
                this.waiters.addLast(moreMemory);
                // loop over and over until we have a buffer or have reserved
                // enough memory to allocate one
                /**
                 * TODO 下面这段循环代码总得分配思路:可能一下子分配不了那么大的内存,但是可以先有点就分配一点,一点点分配
                 * 如果分配的内存的大小,还是没有要申请的内存大小大,内存池就会一直分配内存,一点一点地去分配,等着别人释放内存
                 */
                //size 32K      代表要申请的内存大小
                //accumulated   代表已经申请到的内存大小
                while (accumulated < size) {
                    long startWaitNs = time.nanoseconds();
                    long timeNs;
                    boolean waitingTimeElapsed;
                    try {
                        //在等待,等待别人释放内存
                        //如果这儿的代码是等待wait操作,那么可以想一下当有人释放内存的时候,肯定得唤醒这里的代码
                        //两种情况代码会继续往下执行: 1) 等待时间到了 2) 别人释放内存,然后被唤醒(参见deallocate方法中,释放完内存后会调用signal唤醒等待分配内存的线程)
                        waitingTimeElapsed = !moreMemory.await(remainingTimeToBlockNs, TimeUnit.NANOSECONDS);
                    } catch (InterruptedException e) {
                        this.waiters.remove(moreMemory);
                        throw e;
                    } finally {
                        long endWaitNs = time.nanoseconds();
                        timeNs = Math.max(0L, endWaitNs - startWaitNs);
                        this.waitTime.record(timeNs, time.milliseconds());
                    }

                    if (waitingTimeElapsed) {
                        this.waiters.remove(moreMemory);
                        throw new TimeoutException("Failed to allocate memory within the configured max blocking time " + maxTimeToBlockMs + " ms.");
                    }

                    remainingTimeToBlockNs -= timeNs;
                    // check if we can satisfy this request from the free list,
                    // otherwise allocate memory
                    // 再次看一下,内存池里面有没有可用的内存,并且你申请的内存的大小就是一个批次的大小,有的话就直接分配内存
                    if (accumulated == 0 && size == this.poolableSize && !this.free.isEmpty()) {
                        // just grab a buffer from the free list
                        //有就直接分配内存
                        buffer = this.free.pollFirst();
                        accumulated = size;
                    } else {//内存池里依然没有可用的内存,那么这时候就一点一点地去分配
                        // we'll need to allocate memory, but we may only get
                        // part of what we need on this iteration
                        freeUp(size - accumulated);
                        //可以给你分配的内存
                        int got = (int) Math.min(size - accumulated, this.availableMemory);
                        //内存扣减操作
                        this.availableMemory -= got;
                        //累加已经分配了多少内存
                        accumulated += got;
                    }
                }

                // remove the condition for this thread to let the next thread
                // in line start getting memory
                Condition removed = this.waiters.removeFirst();
                if (removed != moreMemory)
                    throw new IllegalStateException("Wrong condition: this shouldn't happen.");

                // signal any additional waiters if there is more memory left
                // over for them
                if (this.availableMemory > 0 || !this.free.isEmpty()) {
                    if (!this.waiters.isEmpty())
                        this.waiters.peekFirst().signal();
                }

                // unlock and return the buffer
                lock.unlock();
                if (buffer == null)
                    return ByteBuffer.allocate(size);
                else
                    return buffer;
            }
        } finally {
            if (lock.isHeldByCurrentThread())
                lock.unlock();
        }
    }

    /**
     * Attempt to ensure we have at least the requested number of bytes of memory for allocation by deallocating pooled
     * buffers (if needed)
     */
    private void freeUp(int size) {
        while (!this.free.isEmpty() && this.availableMemory < size)
            this.availableMemory += this.free.pollLast().capacity();
    }

    /**
     * Return buffers to the pool. If they are of the poolable size add them to the free list, otherwise just mark the
     * memory as free.
     * 
     * @param buffer The buffer to return
     * @param size The size of the buffer to mark as deallocated, note that this maybe smaller than buffer.capacity
     *             since the buffer may re-allocate itself during in-place compression
     */
    public void deallocate(ByteBuffer buffer, int size) {
        lock.lock();
        try {
            //申请的内存buffer恰好等于默认的poolableSize大小(即一个批次的大小16KB),那么将该buffer添加到内存池队列中
            if (size == this.poolableSize && size == buffer.capacity()) {
                //TODO 清空buffer数据
                buffer.clear();
                //TODO 将buffer添加到内存池的队列中,等待下次其它线程申请复用
                this.free.add(buffer);
            } else {
                //申请的内存buffer不等于默认的大小16KB,也就是说一条消息的大小超过了poolableSize,那么将该buffer大小直接添加到availableMemory中,归为可用内存
                //这时候等着垃圾回收即可
                this.availableMemory += size;
            }
            Condition moreMem = this.waiters.peekFirst();
            if (moreMem != null)
                //TODO 重要点!!!
                // 释放内存或者回收内存以后,都会唤醒等待内存的线程
                // 接下来就要唤醒正在等待分配内存的线程
                moreMem.signal();
        } finally {
            lock.unlock();
        }
    }

    //释放内存池内存
    public void deallocate(ByteBuffer buffer) {
        deallocate(buffer, buffer.capacity());
    }

    /**
     * the total free memory both unallocated and in the free list
     */
    public long availableMemory() {
        lock.lock();
        try {
            return this.availableMemory + this.free.size() * this.poolableSize;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Get the unallocated memory (not in the free list or in use)
     */
    public long unallocatedMemory() {
        lock.lock();
        try {
            return this.availableMemory;
        } finally {
            lock.unlock();
        }
    }

    /**
     * The number of threads blocked waiting on memory
     */
    public int queued() {
        lock.lock();
        try {
            return this.waiters.size();
        } finally {
            lock.unlock();
        }
    }

    /**
     * The buffer size that will be retained in the free list after use
     */
    public int poolableSize() {
        return this.poolableSize;
    }

    /**
     * The total memory managed by this pool
     */
    public long totalMemory() {
        return this.totalMemory;
    }

    // package-private method used only for testing
    Deque<Condition> waiters() {
        return this.waiters;
    }
}
