package com.maxdemarzi;

import org.apache.commons.lang.ArrayUtils;
import org.codehaus.jackson.map.ObjectMapper;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.schema.Schema;
import org.neo4j.tooling.GlobalGraphOperations;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

@Path("/service")
public class Service {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    private HashMap<Long, HashSet<Long>> nodeNeighbors = new HashMap<>(); // which nodes are key's neighbors
    private HashMap<Long, HashMap<Long, Double>> nodeNeighborsWeights = new HashMap<>(); // node neighbor weights
    private HashMap<Integer, HashSet<Long>> nodesInCommunity = new HashMap<>(); // which nodes are in key community
    private HashMap<Integer, HashSet<Long>> nodesInNodeCommunity = new HashMap<>(); // which nodes are in key node community
    private HashMap<Long, Integer> communityForNode = new HashMap<>(); // which community is a node in
    private HashMap<Long, Integer> nodeCommunityForNode = new HashMap<>(); // which node community is a node in
    private HashMap<Integer, Integer> nodeCommunitiesToCommunities = new HashMap<>(); // which communities map to node communities
    private ArrayList<Double> communityWeights;
    private ArrayList<Double> nodeCommunityWeights;
    private long[] providers;
    private int N;
    private double resolution = 1.0;
    private Double graphWeightSum;
    private boolean communityUpdate = false;

    @GET
    @Path("/helloworld")
    public Response helloWorld() throws IOException {
        Map<String, String> results = new HashMap<String,String>(){{
            put("hello","world");
        }};
        return Response.ok().entity(objectMapper.writeValueAsString(results)).build();
    }

    @GET
    @Path("/warmup")
    public Response warmUp(@Context GraphDatabaseService db) throws IOException {
        Map<String, String> results = new HashMap<String,String>(){{
            put("warmed","up");
        }};

        try (Transaction tx = db.beginTx()) {
            for (Node n : GlobalGraphOperations.at(db).getAllNodes()) {
                n.getPropertyKeys();
                for (Relationship relationship : n.getRelationships()) {
                    relationship.getPropertyKeys();
                    relationship.getStartNode();
                }
            }

            for (Relationship relationship : GlobalGraphOperations.at(db).getAllRelationships()) {
                relationship.getPropertyKeys();
                relationship.getNodes();
            }
        }

        return Response.ok().entity(objectMapper.writeValueAsString(results)).build();
    }

    @GET
    @Path("/migrate")
    public Response migrate(@Context GraphDatabaseService db) throws IOException {
        Map<String, String> results = new HashMap<>();

        boolean migrated;
        try (Transaction tx = db.beginTx()) {
            migrated = db.schema().getConstraints().iterator().hasNext();
        }

        if (migrated) {
            results.put("already", "migrated");
        } else {
            // Perform Migration
            try (Transaction tx = db.beginTx()) {
                Schema schema = db.schema();
                schema.indexFor(Labels.Provider)
                        .on("npi")
                        .create();

                schema.indexFor(Labels.Provider)
                        .on("community")
                        .create();

                schema.indexFor(Labels.Provider)
                        .on("nodeCommunity")
                        .create();
                tx.success();
            }
            // Wait for indexes to come online
            try (Transaction tx = db.beginTx()) {
                Schema schema = db.schema();
                schema.awaitIndexesOnline(1, TimeUnit.DAYS);
            }
            results.put("migrat-wait-for-it", "ed");
        }

        return Response.ok().entity(objectMapper.writeValueAsString(results)).build();
    }

    @GET
    @Path("/dump")
    public Response dump(@Context GraphDatabaseService db) throws IOException {
        ArrayList<Map> results = new ArrayList<>();
        try (Transaction tx = db.beginTx()) {
            for (Node n : GlobalGraphOperations.at(db).getAllNodes()) {
                Map<String, Object> result = n.getAllProperties();
                results.add(result);
            }
        }

        return Response.ok().entity(objectMapper.writeValueAsString(results)).build();
    }

    @GET
    @Path("/louvain_method")
    public Response louvainMethod(@Context GraphDatabaseService db) throws ExecutionException, IOException {
        Map<String, String> results = new HashMap<String,String>(){{
            put("louvain method","calculated");
        }};

        // Get the node ids of the Providers in the graph
        providers = getProviders(db);
        // Get the count of Communities in the graph
        N = providers.length;
        System.out.println("After get Provider Count of " + N + " " + new java.util.Date());
        graphWeightSum = weightProviders(db);
        System.out.println("After gettting weight of " + graphWeightSum + " " + new java.util.Date());
        communityWeights = new ArrayList<>(N);
        for (int i = 0; i < N; i++)
        {
            communityWeights.add(0.0);
        }


        // Initialize
        for (int i = 0; i < providers.length; i++) {
            nodesInCommunity.put(i, new HashSet<Long>());
            nodesInCommunity.get(i).add(providers[i]);
            nodesInNodeCommunity.put(i, new HashSet<Long>());
            nodesInNodeCommunity.get(i).add(providers[i]);
            communityForNode.put(providers[i], i);
            nodeCommunityForNode.put(providers[i], i);
            nodeCommunitiesToCommunities.put(i, i);

        }

        nodeCommunityWeights = new ArrayList<>(N);
        for (int i = 0; i < providers.length; i++) {
            nodeCommunityWeights.add(i, getNodeCommunityWeight(i));
        }

        System.out.println("After initialization " + new java.util.Date());

        Random rand = new Random();
        boolean someChange = true;
        while (someChange) {
            System.out.println("In computeModularity OUTER loop of " + N + "  " + new java.util.Date());
            someChange = false;
            boolean localChange = true;
            while (localChange) {
                localChange = false;
                int start = Math.abs(rand.nextInt()) % N;

                int step = 0;
                for (int i = start; step < N; i = (i + 1) % N) {
                    if (step % 1000 == 0) {
                        System.out.println("In computeModularity INNER loop of " + step + "  " + new java.util.Date());
                    }

                    step++;

                    // Find the best community
                    int bestCommunity = updateBestCommunity(i);
                    if ((nodeCommunitiesToCommunities.get(i) != bestCommunity) && (this.communityUpdate)) {
                        moveNodeCommunity(i, bestCommunity);
                        double bestCommunityWeight = communityWeights.get(bestCommunity);

                        bestCommunityWeight += getNodeCommunityWeight(i);
                        communityWeights.set(bestCommunity, bestCommunityWeight);
                        localChange = true;
                    }

                    communityUpdate = false;
                }
                someChange = localChange || someChange;
                System.out.println("--Finished INNER LOOP for " + N + "  step " + step + " "+ new java.util.Date());
            }
            if (someChange)
            {
                System.out.println("In computeModularity ZOOM OUT of " + N + "  " + new java.util.Date());
                zoomOut();
            }
        }
        System.out.println("Writing Communities of " + N + "  " + new java.util.Date());
        writeCommunities(db);

        return Response.ok().entity(objectMapper.writeValueAsString(results)).build();
    }

    private void zoomOut() {
        N = reInitializeCommunities();
        communityWeights = new ArrayList<>(N);
        for (int i = 0; i < N; i++)
        {
            communityWeights.add(getCommunityWeight(i));
        }
    }

    public int reInitializeCommunities() {
        Map<Integer, Integer> initCommunities = new HashMap<>();
        int communityCounter = 0;
        nodesInCommunity.clear();
        nodesInNodeCommunity.clear();
        nodeCommunityWeights = new ArrayList<Double>();
        nodeCommunitiesToCommunities.clear();

        for (Long provider : providers) {
            Integer communityId = communityForNode.get(provider);
            if (!initCommunities.containsKey(communityId))
            {
                initCommunities.put(communityId, communityCounter);
                nodesInCommunity.put(communityCounter, new HashSet<Long>());
                nodesInNodeCommunity.put(communityCounter, new HashSet<Long>());
                communityCounter++;
            }
            int newCommunityId = initCommunities.get(communityId);
            communityForNode.put(provider, newCommunityId);
            nodeCommunityForNode.put(provider, newCommunityId);
            nodeCommunitiesToCommunities.put(newCommunityId, newCommunityId);
            nodesInCommunity.get(newCommunityId).add(provider);
            nodesInNodeCommunity.get(newCommunityId).add(provider);

        }

        for(Integer communityId : initCommunities.values()) {
            nodeCommunityWeights.add(getNodeCommunityWeight(communityId));
        }

        return communityCounter;
    }

    private void writeCommunities(@Context GraphDatabaseService db) {
        int i = 0;
        Transaction tx = db.beginTx();
        try {

            for ( Map.Entry entry : nodesInCommunity.entrySet()) {
                Integer community = (Integer)entry.getKey();
                for (Long nodeId : (HashSet<Long>)entry.getValue()) {
                    Node node = db.getNodeById(nodeId);
                    node.setProperty("community", community);
                    node.setProperty("nodeCommunity", nodeCommunityForNode.get(nodeId));
                    i++;

                    // Commit every x updates
                    if (i % 10000 == 0) {
                        tx.success();
                        tx.close();
                        tx = db.beginTx();
                    }
                }
            }

            tx.success();
        } finally {
            tx.close();
        }
    }

    private void moveNodeCommunity(int nodeCommunity, int toCommunity) {
        int fromCommunity = nodeCommunitiesToCommunities.get(nodeCommunity);
        nodeCommunitiesToCommunities.put(nodeCommunity, toCommunity);
        Set<Long> nodesFromCommunity = nodesInCommunity.get(fromCommunity);

        nodesInCommunity.remove(fromCommunity);
        nodesInCommunity.get(toCommunity).addAll(nodesFromCommunity);

        for (Long nodeId : nodesFromCommunity) {
            nodeCommunitiesToCommunities.put(nodeCommunityForNode.get(nodeId), toCommunity); // I hope?
            communityForNode.put(nodeId, toCommunity);
        }
    }

    private int updateBestCommunity(Integer nodeCommunity ) {
        int bestCommunity = 0;
        double best = 0;
        // Get Communities Connected To Node Communities
        Set<Integer> communities = new HashSet<>();
        for (long nodeId : nodesInNodeCommunity.get(nodeCommunity)) {
            for (long neighborId : nodeNeighbors.get(nodeId)) {
                communities.add(communityForNode.get(neighborId));
            }
        }
        for (Integer community : communities) {
            double qValue = q(nodeCommunity, community);
            if (qValue > best)
            {
                best = qValue;
                bestCommunity = community;
                communityUpdate = true;
            }
        }
        return bestCommunity;
    }

    private double q(Integer nodeCommunity, Integer community) {
        double edgesInCommunity = getEdgesInsideCommunity(nodeCommunity, community);
        double communityWeight = communityWeights.get(community);
        double nodeWeight = nodeCommunityWeights.get(nodeCommunity);//  getNodeCommunityWeight(nodeCommunity);
        double qValue = resolution * edgesInCommunity - (nodeWeight * communityWeight)
                / (2.0 * graphWeightSum);
        int actualNodeCom = nodeCommunitiesToCommunities.get(nodeCommunity);
        int communitySize = nodesInCommunity.get(community).size();

        if ((actualNodeCom == community) && (communitySize > 1))
        {
            qValue = resolution * edgesInCommunity - (nodeWeight * (communityWeight - nodeWeight))
                    / (2.0 * graphWeightSum);
        }
        if ((actualNodeCom == community) && (communitySize == 1))
        {
            qValue = 0.0;
        }
        return qValue;
    }

    private double getCommunityWeight(Integer community)
    {
        double communityWeight = 0.0;
        for (Long communityNode : nodesInCommunity.get(community)) {
            for (Double weight : nodeNeighborsWeights.get(communityNode).values()) {
                communityWeight += weight;
            }
        }

        return communityWeight;
    }

    private double getNodeCommunityWeight(Integer nodeCommunity)
    {
        double communityWeight = 0.0;
        for (Long communityNode : nodesInNodeCommunity.get(nodeCommunity)) {
            for (Double weight : nodeNeighborsWeights.get(communityNode).values()) {
                communityWeight += weight;
            }
        }

        return communityWeight;
    }

    private double getEdgesInsideCommunity(Integer nodeCommunity, Integer community) {
        Set<Long> nodeCommunityNodes = nodesInNodeCommunity.get(nodeCommunity);
        Set<Long> communityNodes = nodesInCommunity.get(community);
        double edges = 0;
        for (Long nodeCommunityNode : nodeCommunityNodes)
        {
            HashSet<Long> nodes = nodeNeighbors.get(nodeCommunityNode);
            HashMap<Long, Double> weights = nodeNeighborsWeights.get(nodeCommunityNode);

            if (nodes.size() <= communityNodes.size()) {
                for (Long node : nodes) {
                    if (communityNodes.contains(node)) {
                        edges += weights.get(node);
                    }
                }
            } else {
                for (Long communityNode : communityNodes) {
                    if (nodes.contains(communityNode)) {
                        edges += weights.get(communityNode);
                    }
                }
            }
        }
        return edges;
    }

    private long[] getProviders(GraphDatabaseService db) {
        ArrayList<Long> providerList = new ArrayList<>();
        try (Transaction tx = db.beginTx()) {
            ResourceIterator<Node> providers = db.findNodes(Labels.Provider);
            while (providers.hasNext()) {
                HashSet<Long> neighbors = new HashSet<>();
                HashMap<Long, Double> neighborWeights = new HashMap<>();
                Node provider = providers.next();
                providerList.add(provider.getId());
                for (Relationship rel : provider.getRelationships(RelationshipTypes.SHARE_MEMBER, Direction.BOTH)) {
                    neighbors.add(rel.getOtherNode(provider).getId());
                    neighborWeights.put(rel.getOtherNode(provider).getId(), (double) rel.getProperty("weight", 0.0D));
                }
                nodeNeighbors.put(provider.getId(), neighbors);
                nodeNeighborsWeights.put(provider.getId(), neighborWeights);
            }
            tx.success();
        }

        return ArrayUtils.toPrimitive(providerList.toArray(new Long[providerList.size()]));
    }

    private double weightProviders(GraphDatabaseService db) {
        double sum = 0.0;
        try (Transaction tx = db.beginTx()) {
            for (Relationship relationship : GlobalGraphOperations.at(db).getAllRelationships()) {
                if (relationship.isType(RelationshipTypes.SHARE_MEMBER)) {
                    sum += (double)relationship.getProperty("weight", 0.0);
                }
            }
            tx.success();
        }

        return sum;
    }
}
