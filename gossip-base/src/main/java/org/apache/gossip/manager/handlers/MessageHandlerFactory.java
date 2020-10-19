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

package org.apache.gossip.manager.handlers;

import org.apache.gossip.manager.GossipCore;
import org.apache.gossip.manager.GossipManager;
import org.apache.gossip.model.*;

import java.util.Arrays;

public class MessageHandlerFactory {

  /**
   * 响应消息处理器，宕机消息处理，节点数据消息，共享数据消息，保活消息，批量节点数据消息，批量节点共享数据。
   * @return
   */
  public static MessageHandler defaultHandler() {
    return concurrentHandler(
        new TypedMessageHandler(Response.class, new ResponseHandler()),
        new TypedMessageHandler(ShutdownMessage.class, new ShutdownMessageHandler()),
        new TypedMessageHandler(PerNodeDataMessage.class, new PerNodeDataMessageHandler()),
        new TypedMessageHandler(SharedDataMessage.class, new SharedDataMessageHandler()),
        new TypedMessageHandler(ActiveGossipMessage.class, new ActiveGossipMessageHandler()),
        new TypedMessageHandler(PerNodeDataBulkMessage.class, new PerNodeDataBulkMessageHandler()),
        new TypedMessageHandler(SharedDataBulkMessage.class, new SharedDataBulkMessageHandler())
    );
  }

  /**
   * @param handlers
   * @return
   */
  public static MessageHandler concurrentHandler(MessageHandler... handlers) {
    if (handlers == null)
      throw new NullPointerException("handlers cannot be null");
    if (Arrays.asList(handlers).stream().filter(i -> i != null).count() != handlers.length) {
      throw new NullPointerException("found at least one null handler");
    }
    return new MessageHandler() {
      @Override public boolean invoke(GossipCore gossipCore, GossipManager gossipManager,
              Base base) {
        // return true if at least one of the component handlers return true.
        //处理消息
        return Arrays.asList(handlers).stream()
                .filter((mi) -> mi.invoke(gossipCore, gossipManager, base)).count() > 0;
      }
    };
  }
}

