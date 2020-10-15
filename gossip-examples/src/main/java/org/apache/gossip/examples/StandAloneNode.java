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

import org.apache.gossip.manager.GossipManager;

/**
 * 简单的维护gossip集群的示例
 */
public class StandAloneNode extends StandAloneExampleBase {

  /**
   * 是否从控制台读取消息
   */
  private static boolean WILL_READ = true;

  public static void main(String[] args) throws InterruptedException, IOException {
    StandAloneNode example = new StandAloneNode(args);
    example.exec(WILL_READ);
  }

  public StandAloneNode(String[] args) {
    args = super.checkArgsForClearFlag(args);
    super.initGossipManager(args);
  }

  @Override
  void printValues(GossipManager gossipService) {
  }

}
