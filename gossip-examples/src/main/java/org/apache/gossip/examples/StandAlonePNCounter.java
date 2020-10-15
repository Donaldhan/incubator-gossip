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
package org.apache.gossip.examples;

import java.io.IOException;

import org.apache.gossip.crdt.PNCounter;
import org.apache.gossip.manager.GossipManager;
import org.apache.gossip.model.SharedDataMessage;

public class StandAlonePNCounter extends StandAloneExampleBase {
  /**
   * 共享集合
   */
  private static final String INDEX_KEY_FOR_PN_COUNTER = "gossip_crdt_pn_counter";
  public static void main(String[] args) throws InterruptedException, IOException {
    StandAlonePNCounter example = new StandAlonePNCounter(args);
    boolean willRead = true;
    example.exec(willRead);
  }

  StandAlonePNCounter(String[] args) {
    args = super.checkArgsForClearFlag(args);
    super.initGossipManager(args);
  }

  @Override
  void printValues(GossipManager gossipService) {
    System.out.println("Last Input: " + getLastInput());
    System.out.println("---------- " + (gossipService.findCrdt(INDEX_KEY_FOR_PN_COUNTER) == null ? ""
            : gossipService.findCrdt(INDEX_KEY_FOR_PN_COUNTER).value()));
    System.out.println("********** " + gossipService.findCrdt(INDEX_KEY_FOR_PN_COUNTER));
  }

  @Override
  boolean processReadLoopInput(String line) {
    char op = line.charAt(0);
    char blank = line.charAt(1);
    String val = line.substring(2);
    Long l = null;
    boolean valid = true;
    try {
      l = Long.valueOf(val);
    } catch (NumberFormatException ex) {
      valid = false;
    }
    valid = valid && ((blank == ' ') && ((op == 'i') || (op == 'd')));
    if (valid) {
      if (op == 'i') {
        increment(l, getGossipManager());
      } else if (op == 'd') {
        decrement(l, getGossipManager());
      }
    }
    return valid;
  }

  /**
   * 增加计数器
   * @param l
   * @param gossipManager
   */
  void increment(Long l, GossipManager gossipManager) {
    PNCounter c = (PNCounter) gossipManager.findCrdt(INDEX_KEY_FOR_PN_COUNTER);
    if (c == null) {
      c = new PNCounter(new PNCounter.Builder(gossipManager).increment((l)));
    } else {
      c = new PNCounter(c, new PNCounter.Builder(gossipManager).increment((l)));
    }
    SharedDataMessage m = new SharedDataMessage();
    m.setExpireAt(Long.MAX_VALUE);
    m.setKey(INDEX_KEY_FOR_PN_COUNTER);
    m.setPayload(c);
    m.setTimestamp(System.currentTimeMillis());
    gossipManager.merge(m);
  }

  /**
   * 减少技术器
   * @param l
   * @param gossipManager
   */
  void decrement(Long l, GossipManager gossipManager) {
    PNCounter c = (PNCounter) gossipManager.findCrdt(INDEX_KEY_FOR_PN_COUNTER);
    if (c == null) {
      c = new PNCounter(new PNCounter.Builder(gossipManager).decrement((l)));
    } else {
      c = new PNCounter(c, new PNCounter.Builder(gossipManager).decrement((l)));
    }
    SharedDataMessage m = new SharedDataMessage();
    m.setExpireAt(Long.MAX_VALUE);
    m.setKey(INDEX_KEY_FOR_PN_COUNTER);
    m.setPayload(c);
    m.setTimestamp(System.currentTimeMillis());
    gossipManager.merge(m);
  }

}