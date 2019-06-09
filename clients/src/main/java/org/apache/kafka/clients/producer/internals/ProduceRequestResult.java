/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
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

import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.RecordBatch;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;


/**
 * A class that models the future completion of a produce request for a single partition. There is one of these per
 * partition in a produce request and it is shared by all the {@link RecordMetadata} instances that are batched together
 * for the same partition in the request.
 *
 *  ProduceRequestResult 并未实现java.util.concurrent.Future接口，但是其通过包含一个count值为1的CountDownLatch对象，
 *  实现了类似于Future的功能
 *
 *  当RecordBatch中全部的消息被正常响应、或超时、或关闭生产者时，会调用ProduceRequestResult.done（）方法，
 *  将produceFuture标记为完成并通过ProduceRequestResult.error字段区分“异常完成”还是“正常完成”，之后调用CountDownLatch对象的
 *  countDown(方法。此时，会唤醒阻塞在CountDownI atch对象的await(方法的线程(这些线程通过ProduceRequestResult的await方法等待上述三个事件的发生)。
 */
public class ProduceRequestResult {

    /**
     * 包含了一个count值为1的CountDownLatch对象，用于实现与Future类似的功能
     */
    private final CountDownLatch latch = new CountDownLatch(1);
    private final TopicPartition topicPartition;

    /**
     *  服务端为此ProducerBatch中的第一条消息分配的offset
     *  在ProduceRequestResult中还有一个需要注意的字段baseOfset,表示的是服务端为此RecordBatch中第一- 条
     *  消息分配的offset,这样每个消息可以根据此offset以及自身在此RecordBatch中的相对偏移量，计算出其在服务端分区中
     *  的偏移量了。
     */
    private volatile Long baseOffset = null;
    private volatile long logAppendTime = RecordBatch.NO_TIMESTAMP;
    /**
     * 区分异常完成还是正常完成
     */
    private volatile RuntimeException error;

    /**
     * Create an instance of this class.
     *
     * @param topicPartition The topic and partition to which this record set was sent was sent
     */
    public ProduceRequestResult(TopicPartition topicPartition) {
        this.topicPartition = topicPartition;
    }

    /**
     * Set the result of the produce request.
     *
     * @param baseOffset The base offset assigned to the record
     * @param logAppendTime The log append time or -1 if CreateTime is being used
     * @param error The error that occurred if there was one, or null
     */
    public void set(long baseOffset, long logAppendTime, RuntimeException error) {
        this.baseOffset = baseOffset;
        this.logAppendTime = logAppendTime;
        this.error = error;
    }

    /**
     * Mark this request as complete and unblock any threads waiting on its completion.
     *
     * ProducerBatch中全部的消息被正常响应、或超时、或关闭生产者时，会调用done()释放锁，将produceFuture标记为已完成，
     * 并通过ProduceRequestResult.error字段区分“异常完成”还是“正常完成”
     */
    public void done() {
        if (baseOffset == null)
            throw new IllegalStateException("The method `set` must be invoked before this method.");

        // 醒阻塞在CountDownLatch对象的线程
        this.latch.countDown();
    }

    /**
     * Await the completion of this request
     */
    public void await() throws InterruptedException {
        // 外部线程通过await方法等待ProducerBatch中全部的消息被正常响应、或超时、或关闭生产者三个事件的发生
        latch.await();
    }

    /**
     * Await the completion of this request (up to the given time interval)
     * @param timeout The maximum time to wait
     * @param unit The unit for the max time
     * @return true if the request completed, false if we timed out
     */
    public boolean await(long timeout, TimeUnit unit) throws InterruptedException {
        return latch.await(timeout, unit);
    }

    /**
     * The base offset for the request (the first offset in the record set)
     */
    public long baseOffset() {
        return baseOffset;
    }

    /**
     * Return true if log append time is being used for this topic
     */
    public boolean hasLogAppendTime() {
        return logAppendTime != RecordBatch.NO_TIMESTAMP;
    }

    /**
     * The log append time or -1 if CreateTime is being used
     */
    public long logAppendTime() {
        return logAppendTime;
    }

    /**
     * The error thrown (generally on the server) while processing this request
     */
    public RuntimeException error() {
        return error;
    }

    /**
     * The topic and partition to which the record was appended
     */
    public TopicPartition topicPartition() {
        return topicPartition;
    }

    /**
     * Has the request completed?
     */
    public boolean completed() {
        return this.latch.getCount() == 0L;
    }
}
