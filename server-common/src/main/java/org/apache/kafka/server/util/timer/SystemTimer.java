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
package org.apache.kafka.server.util.timer;

import org.apache.kafka.common.utils.KafkaThread;
import org.apache.kafka.common.utils.ThreadUtils;
import org.apache.kafka.common.utils.Time;

import java.util.concurrent.DelayQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class SystemTimer implements Timer {
    public static final String SYSTEM_TIMER_THREAD_PREFIX = "executor-";

    // 执行到期任务的线程池（单线程）
    private final ExecutorService taskExecutor;
    // 存储所有活跃的时间格，用于精准推进时间轮
    private final DelayQueue<TimerTaskList> delayQueue;
    // 统计所有层级时间轮中的任务总数。
    private final AtomicInteger taskCounter;
    // 最底层时间轮的引用，其他层级通过overflowWheel间接引用。
    private final TimingWheel timingWheel;

    // Locks used to protect data structures while ticking
    // 保护currentTime的并发修改。
    private final ReentrantReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    private final ReentrantReadWriteLock.ReadLock readLock = readWriteLock.readLock();
    private final ReentrantReadWriteLock.WriteLock writeLock = readWriteLock.writeLock();

    public SystemTimer(String executorName) {
        this(executorName, 1, 20, Time.SYSTEM.hiResClockMs());
    }

    public SystemTimer(
        String executorName,
        long tickMs,
        int wheelSize,
        long startMs
    ) {
        this.taskExecutor = Executors.newFixedThreadPool(1,
            runnable -> KafkaThread.nonDaemon(SYSTEM_TIMER_THREAD_PREFIX + executorName, runnable));
        this.delayQueue = new DelayQueue<>();
        this.taskCounter = new AtomicInteger(0);
        this.timingWheel = new TimingWheel(
            tickMs,
            wheelSize,
            startMs,
            taskCounter,
            delayQueue
        );
    }

    /**
     * 添加延迟任务
     * @param timerTask the task to add
     */
    public void add(TimerTask timerTask) {
        readLock.lock();
        try {
            addTimerTaskEntry(new TimerTaskEntry(timerTask, timerTask.delayMs + Time.SYSTEM.hiResClockMs()));
        } finally {
            readLock.unlock();
        }
    }

    private void addTimerTaskEntry(TimerTaskEntry timerTaskEntry) {
        // 添加到时间轮
        if (!timingWheel.add(timerTaskEntry)) {
            // Already expired or cancelled
            // 如果添加时间轮失败，并且没有取消，说明已过期，直接提交给执行器执行
            if (!timerTaskEntry.cancelled()) {
                taskExecutor.submit(timerTaskEntry.timerTask);
            }
        }
    }

    /**
     * Advances the clock if there is an expired bucket. If there isn't any expired bucket when called,
     * waits up to timeoutMs before giving up.
     */
    public boolean advanceClock(long timeoutMs) throws InterruptedException {
        // 弹出对应的bucket
        TimerTaskList bucket = delayQueue.poll(timeoutMs, TimeUnit.MILLISECONDS);
        if (bucket != null) {
            writeLock.lock();
            try {
                while (bucket != null) {
                    // 滚动时间
                    timingWheel.advanceClock(bucket.getExpiration());
                    // 滚动任务，将任务重新插入时间轮，如果过期则触发执行
                    bucket.flush(this::addTimerTaskEntry);
                    bucket = delayQueue.poll();
                }
            } finally {
                writeLock.unlock();
            }
            return true;
        } else {
            return false;
        }
    }

    public int size() {
        return taskCounter.get();
    }

    @Override
    public void close() {
        ThreadUtils.shutdownExecutorServiceQuietly(taskExecutor, 5, TimeUnit.SECONDS);
    }

    // visible for testing
    boolean isTerminated() {
        return taskExecutor.isTerminated();
    }
}
