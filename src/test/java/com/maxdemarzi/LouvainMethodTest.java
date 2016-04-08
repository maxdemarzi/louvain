package com.maxdemarzi;

import org.junit.Rule;
import org.junit.Test;
import org.neo4j.harness.junit.Neo4jRule;
import org.neo4j.test.server.HTTP;

import java.util.ArrayList;
import java.util.HashMap;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertTrue;

public class LouvainMethodTest {
    @Rule
    public Neo4jRule neo4j = new Neo4jRule()
            .withFixture(MODEL_STATEMENT)
            .withExtension("/v1", Service.class);

    public static final String MODEL_STATEMENT =
            new StringBuilder()
                    .append("CREATE (p1:Provider {providerId: 'p1'})")
                    .append("CREATE (p2:Provider {providerId: 'p2'})")
                    .append("MERGE (p1)-[:SHARE_MEMBER {weight:0.2}]->(p2)")
                    .append("CREATE (p3:Provider {providerId: 'p3'})")
                    .append("CREATE (p4:Provider {providerId: 'p4'})")
                    .append("MERGE (p3)-[:SHARE_MEMBER {weight:0.2}]->(p4)")
                    .append("CREATE (p5:Provider {providerId: 'p5'})")
                    .append("CREATE (p6:Provider {providerId: 'p6'})")
                    .append("CREATE (p7:Provider {providerId: 'p7'})")
                    .append("CREATE (p8:Provider {providerId: 'p8'})")
                    .append("MERGE (p5)-[:SHARE_MEMBER {weight:0.05}]->(p6)")
                    .append("MERGE (p5)-[:SHARE_MEMBER {weight:0.1}]->(p7)")
                    .append("MERGE (p5)-[:SHARE_MEMBER {weight:0.1}]->(p8)")
                    .append("MERGE (p6)-[:SHARE_MEMBER {weight:0.1}]->(p7)")
                    .append("MERGE (p6)-[:SHARE_MEMBER {weight:0.1}]->(p8)")
                    .append("MERGE (p7)-[:SHARE_MEMBER {weight:0.15}]->(p8)")
                    .append("CREATE (p9:Provider {providerId: 'p9'})")
                    .toString();

    @Test
    public void shouldRespondToLouvainMethod() {
        HTTP.Response response = HTTP.GET(neo4j.httpURI().resolve("/v1/service/louvain_method").toString());
        HashMap actual = response.content();
        assertTrue(actual.equals(expected));

        response = HTTP.GET(neo4j.httpURI().resolve("/v1/service/dump").toString());
        ArrayList<HashMap<String, Object>> results  = response.content();
        assertArrayEquals(nodes.toArray(), results.toArray());
    }

    private static final HashMap expected = new HashMap<String, Object>() {{
        put("louvain method","calculated");
    }};

    private static final ArrayList nodes = new ArrayList() {{
        add(new HashMap<String,Object>() {{
            put("providerId", "p1");
            put("community", 0);
            put("nodeCommunity", 0);
        }});
        add(new HashMap<String,Object>() {{
            put("providerId", "p2");
            put("community", 0);
            put("nodeCommunity", 0);
        }});
        add(new HashMap<String,Object>() {{
            put("providerId", "p3");
            put("community", 1);
            put("nodeCommunity", 1);
        }});
        add(new HashMap<String,Object>() {{
            put("providerId", "p4");
            put("community", 1);
            put("nodeCommunity", 1);

        }});
        add(new HashMap<String,Object>() {{
            put("providerId", "p5");
            put("community", 2);
            put("nodeCommunity", 2);
        }});
        add(new HashMap<String,Object>() {{
            put("providerId", "p6");
            put("community", 2);
            put("nodeCommunity", 2);

        }});
        add(new HashMap<String,Object>() {{
            put("providerId", "p7");
            put("community", 2);
            put("nodeCommunity", 2);
        }});
        add(new HashMap<String,Object>() {{
            put("providerId", "p8");
            put("community", 2);
            put("nodeCommunity", 2);
        }});
        add(new HashMap<String,Object>() {{
            put("providerId", "p9");
            put("community", 3);
            put("nodeCommunity", 3);
        }});
    }};
}
