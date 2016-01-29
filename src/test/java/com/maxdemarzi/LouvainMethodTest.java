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
                    .append("CREATE (p1:Provider {name: 'p1'})")
                    .append("CREATE (p2:Provider {name: 'p2'})")
                    .append("MERGE (p1)-[:SHARE_MEMBER {count:40, weight:0.2}]->(p2)")
                    .append("CREATE (p3:Provider {name: 'p3'})")
                    .append("CREATE (p4:Provider {name: 'p4'})")
                    .append("MERGE (p3)-[:SHARE_MEMBER {count:40, weight:0.2}]->(p4)")
                    .append("CREATE (p5:Provider {name: 'p5'})")
                    .append("CREATE (p6:Provider {name: 'p6'})")
                    .append("CREATE (p7:Provider {name: 'p7'})")
                    .append("CREATE (p8:Provider {name: 'p8'})")
                    .append("MERGE (p5)-[:SHARE_MEMBER {count:10, weight:0.05}]->(p6)")
                    .append("MERGE (p5)-[:SHARE_MEMBER {count:20, weight:0.1}]->(p7)")
                    .append("MERGE (p5)-[:SHARE_MEMBER {count:20, weight:0.1}]->(p8)")
                    .append("MERGE (p6)-[:SHARE_MEMBER {count:20, weight:0.1}]->(p7)")
                    .append("MERGE (p6)-[:SHARE_MEMBER {count:20, weight:0.1}]->(p8)")
                    .append("MERGE (p7)-[:SHARE_MEMBER {count:30, weight:0.15}]->(p8)")
                    .append("CREATE (p9:Provider {name: 'p9'})")
                    .toString();

    @Test
    public void shouldRespondToLouvainMethod() {
        //HTTP.GET(neo4j.httpURI().resolve("/v1/service/migrate").toString());
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
            put("name", "p1");
            put("community", 0);
            put("nodeCommunity", 0);
        }});
        add(new HashMap<String,Object>() {{
            put("name", "p2");
            put("community", 0);
            put("nodeCommunity", 0);
        }});
        add(new HashMap<String,Object>() {{
            put("name", "p3");
            put("community", 1);
            put("nodeCommunity", 1);
        }});
        add(new HashMap<String,Object>() {{
            put("name", "p4");
            put("community", 1);
            put("nodeCommunity", 1);

        }});
        add(new HashMap<String,Object>() {{
            put("name", "p5");
            put("community", 2);
            put("nodeCommunity", 2);
        }});
        add(new HashMap<String,Object>() {{
            put("name", "p6");
            put("community", 2);
            put("nodeCommunity", 2);

        }});
        add(new HashMap<String,Object>() {{
            put("name", "p7");
            put("community", 2);
            put("nodeCommunity", 2);
        }});
        add(new HashMap<String,Object>() {{
            put("name", "p8");
            put("community", 2);
            put("nodeCommunity", 2);
        }});
        add(new HashMap<String,Object>() {{
            put("name", "p9");
            put("community", 3);
            put("nodeCommunity", 3);
        }});
    }};
}
