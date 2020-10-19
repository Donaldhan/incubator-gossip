/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.gossip.manager;

import org.apache.gossip.GossipSettings;
import org.apache.gossip.LocalMember;
import org.apache.gossip.event.GossipListener;
import org.apache.gossip.event.GossipState;
import org.apache.gossip.model.PerNodeDataMessage;
import org.apache.gossip.model.ShutdownMessage;
import org.apache.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.*;
import java.util.function.BiFunction;

/**
 * 成员状态刷新线程
 */
public class GossipMemberStateRefresher {
  public static final Logger LOGGER = Logger.getLogger(GossipMemberStateRefresher.class);

  /**
   * gossip成员
   */
  private final Map<LocalMember, GossipState> members;
  /**
   * gossip配置
   */
  private final GossipSettings settings;
  /**
   * gossip监听器
   */
  private final List<GossipListener> listeners = new CopyOnWriteArrayList<>();
  /**
   * 系统时钟
   */
  private final Clock clock;
  private final BiFunction<String, String, PerNodeDataMessage> findPerNodeGossipData;
  /**
   * 监听器执行线程
   */
  private final ExecutorService listenerExecutor;
  /**
   * 条赌气
   */
  private final ScheduledExecutorService scheduledExecutor;
  /**
   * 任务队列
   */
  private final BlockingQueue<Runnable> workQueue;

  public GossipMemberStateRefresher(Map<LocalMember, GossipState> members, GossipSettings settings,
                                    GossipListener listener,
                                    BiFunction<String, String, PerNodeDataMessage> findPerNodeGossipData) {
    this.members = members;
    this.settings = settings;
    listeners.add(listener);
    this.findPerNodeGossipData = findPerNodeGossipData;
    clock = new SystemClock();
    workQueue = new ArrayBlockingQueue<>(1024);
    listenerExecutor = new ThreadPoolExecutor(1, 20, 1, TimeUnit.SECONDS, workQueue,
            new ThreadPoolExecutor.DiscardOldestPolicy());
    scheduledExecutor = Executors.newScheduledThreadPool(1);
  }

  /**
   * 调度成员状态刷新器
   */
  public void init() {
    scheduledExecutor.scheduleAtFixedRate(() -> run(), 0, 100, TimeUnit.MILLISECONDS);
  }

  /**
   *
   */
  public void run() {
    try {
      runOnce();
    } catch (RuntimeException ex) {
      LOGGER.warn("scheduled state had exception", ex);
    }
  }

  /**
   * gossip成员状态探测
   */
  public void runOnce() {
    for (Entry<LocalMember, GossipState> entry : members.entrySet()) {
      boolean userDown = processOptimisticShutdown(entry);
      if (userDown)
        continue;

      Double phiMeasure = entry.getKey().detect(clock.nanoTime());
      GossipState requiredState;
      //根据探测结果，判断节点状态
      if (phiMeasure != null) {
        requiredState = calcRequiredState(phiMeasure);
      } else {
        requiredState = calcRequiredStateCleanupInterval(entry.getKey(), entry.getValue());
      }

      if (entry.getValue() != requiredState) {
        members.put(entry.getKey(), requiredState);
        /* Call listeners asynchronously 异步触发节点状态监听器*/
        for (GossipListener listener: listeners)
          listenerExecutor.execute(() -> listener.gossipEvent(entry.getKey(), requiredState));
      }
    }
  }

  /**
   *
   * @param phiMeasure
   * @return
   */
  public GossipState calcRequiredState(Double phiMeasure) {
    //如果探测节点存活的时间间隔大于阈值，则节点down
    if (phiMeasure > settings.getConvictThreshold())
      return GossipState.DOWN;
    else
      return GossipState.UP;
  }

  /**
   * 首次探活状态
   * @param member
   * @param state
   * @return
   */
  public GossipState calcRequiredStateCleanupInterval(LocalMember member, GossipState state) {
    long now = clock.nanoTime();
    long nowInMillis = TimeUnit.MILLISECONDS.convert(now, TimeUnit.NANOSECONDS);
    //如果当前时间减去清理时间间隔，大于成员的心跳时间，则DOWN
    if (nowInMillis - settings.getCleanupInterval() > member.getHeartbeat()) {
      return GossipState.DOWN;
    } else {
      return state;
    }
  }

  /**
   * If we have a special key the per-node data that means that the node has sent us
   * a pre-emptive shutdown message. We process this so node is seen down sooner
   * 预处理节点状态
   * @param l member to consider
   * @return true if node forced down
   */
  public boolean processOptimisticShutdown(Entry<LocalMember, GossipState> l) {
    //获取节点宕机消息
    PerNodeDataMessage m = findPerNodeGossipData.apply(l.getKey().getId(), ShutdownMessage.PER_NODE_KEY);
    if (m == null) {
      return false;
    }
    ShutdownMessage s = (ShutdownMessage) m.getPayload();
    //如果节点宕机时间大于当前心跳时间
    if (s.getShutdownAtNanos() > l.getKey().getHeartbeat()) {
      members.put(l.getKey(), GossipState.DOWN);
      if (l.getValue() == GossipState.UP) {
        //如果状态变化，则通知监听器
        for (GossipListener listener: listeners)
          listenerExecutor.execute(() -> listener.gossipEvent(l.getKey(), GossipState.DOWN));
      }
      return true;
    }
    return false;
  }

  /**
   * 注解监听器
   * @param listener
   */
  public void register(GossipListener listener) {
    listeners.add(listener);
  }

  /**
   * 关闭gossip成员刷新器
   */
  public void shutdown() {
    scheduledExecutor.shutdown();
    try {
      scheduledExecutor.awaitTermination(5, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      LOGGER.debug("Issue during shutdown", e);
    }
    listenerExecutor.shutdown();
    try {
      listenerExecutor.awaitTermination(5, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      LOGGER.debug("Issue during shutdown", e);
    }
    listenerExecutor.shutdownNow();
  }
}