package org.home.vt.gossip;

import lombok.extern.slf4j.Slf4j;
import org.apache.gossip.examples.StandAloneNode;
import org.junit.Test;

import java.io.IOException;

/**
 * @ClassName: TestStandAloneNode
 * @Description:
 * @Author: VT
 * @Date: 2020-10-14 16:41
 */
@Slf4j
public class TestStandAloneNode {
    /**
     * This sets up a single StandAloneNode that starts out listening to itself. The arguments are:
     * 1. The URI (host and port) for the node - **udp://localhost:10000**
     * 2. The id for the node - **0**
     * 3. The URI for a "seed" node - **udp://localhost:10000**
     * 4. The id for that seed node - **0**
     */
    @Test
    public void testStandAloneNode(){
        try {
            String uri = "udp://localhost:10000";
            String nodeId = "0";
            String seedUri = "udp://localhost:10000";
            String seedNodeId = "0";
            String[] args = new String[]{uri, nodeId, seedUri, seedNodeId};
            StandAloneNode example = new StandAloneNode(args);
            //读取控制台消息
            example.exec(true);
        } catch (IOException e) {
            log.error("testStandAloneNode error",e);
        }
    }
}
