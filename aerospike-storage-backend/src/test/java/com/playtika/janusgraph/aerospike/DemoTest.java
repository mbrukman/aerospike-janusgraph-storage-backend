package com.playtika.janusgraph.aerospike;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.*;
import org.janusgraph.core.schema.ConsistencyModifier;
import org.janusgraph.core.schema.JanusGraphIndex;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.junit.Test;

import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import static com.playtika.janusgraph.aerospike.AerospikeStoreManager.AEROSPIKE_BUFFER_SIZE;
import static com.playtika.janusgraph.aerospike.ConfigOptions.ALLOW_SCAN;
import static java.util.Arrays.asList;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.*;

public class DemoTest {

    public static final String CREDENTIAL_TYPE = "credentialType";
    public static final String CREDENTIAL_VALUE = "credentialValue";
    public static final String PLATFORM_IDENTITY_PARENT_EVENT = "pltfParentEvent";
    public static final String APP_IDENTITY_PARENT_EVENT = "appParentEvent";
    public static final String APP_IDENTITY_ID = "appIdentityId";

    @Test
    public void demo(){

        while(true) {
            AerospikeTestUtils.deleteAllRecords("test");

            ModifiableConfiguration config = buildGraphConfiguration();
            config.set(STORAGE_BACKEND, "com.playtika.janusgraph.aerospike.AerospikeStoreManager");
            //!!! need to prevent small batches mutations as we use deferred locking approach !!!
            config.set(BUFFER_SIZE, AEROSPIKE_BUFFER_SIZE);
            config.set(ALLOW_SCAN, true);  //for test purposes only

            JanusGraph graph = JanusGraphFactory.open(config);

            //Create Schema
            JanusGraphManagement management = graph.openManagement();
            final PropertyKey credentialType = management.makePropertyKey(CREDENTIAL_TYPE).dataType(String.class).make();
            final PropertyKey credentialValue = management.makePropertyKey(CREDENTIAL_VALUE).dataType(String.class).make();
            final PropertyKey appIdentityId = management.makePropertyKey(APP_IDENTITY_ID).dataType(String.class).make();

            JanusGraphIndex platformIdentityIndex = management.buildIndex("platformIdentity", Vertex.class)
                    .addKey(credentialType).addKey(credentialValue)
                    .unique()
                    .buildCompositeIndex();
            management.setConsistency(platformIdentityIndex, ConsistencyModifier.LOCK);

            JanusGraphIndex appIdentityIndex = management.buildIndex("appIdentity", Vertex.class)
                    .addKey(appIdentityId)
                    .unique()
                    .buildCompositeIndex();
            management.setConsistency(appIdentityIndex, ConsistencyModifier.LOCK);

            management.makeEdgeLabel(PLATFORM_IDENTITY_PARENT_EVENT).multiplicity(Multiplicity.MANY2ONE).make();
            management.makeEdgeLabel(APP_IDENTITY_PARENT_EVENT).multiplicity(Multiplicity.ONE2ONE).make();
            management.commit();

            List<CredentialIdentity> creds = asList(
                    new CredentialIdentity("fb", "qd3qeqda3123dwq"),
                    new CredentialIdentity("one", "dwdw@cwd.com"));

            AerospikeStoreManager.fails = true;
            Vertex v1Result = getOrCreateEvent(graph, "123:456", creds);

            AerospikeStoreManager.fails = false;
            Vertex v2Result = getOrCreateEvent(graph, "123:456", creds);

            //query by indexed property
            JanusGraphTransaction tx = graph.newTransaction();

            Vertex searchCred = tx.query()
                    .has(CREDENTIAL_TYPE, "fb")
                    .has(CREDENTIAL_VALUE, "qd3qeqda3123dwq").vertices().iterator().next();

            if(searchCred.edges(Direction.OUT, PLATFORM_IDENTITY_PARENT_EVENT).hasNext()) {
                Vertex searchResult = searchCred.edges(Direction.OUT, PLATFORM_IDENTITY_PARENT_EVENT).next().inVertex();
            } else {
                int debug = 0;
            }

            graph.close();
        }
    }

    private Vertex getOrCreateEvent(JanusGraph graph, String appIdentityString, List<CredentialIdentity> credentials){
        JanusGraphTransaction tx = graph.newTransaction();

        // vertices
        List<Vertex> platformIdentities = credentials.stream()
                .map(cred -> getOrCreateVertex(tx, CREDENTIAL_TYPE, cred.credType, CREDENTIAL_VALUE, cred.credValue))
                .collect(Collectors.toList());;

        Vertex appIdentity = getOrCreateVertex(tx, APP_IDENTITY_ID, appIdentityString);

        //check vertices for parent event
        Vertex parentEvent = null;
        for(Vertex platformIdentity : platformIdentities){
            Iterator<Edge> edges = platformIdentity.edges(Direction.OUT, PLATFORM_IDENTITY_PARENT_EVENT);
            if(edges.hasNext()){
                parentEvent = edges.next().inVertex();
            }
        }
        Iterator<Edge> appIdentityEdges = appIdentity.edges(Direction.OUT, APP_IDENTITY_PARENT_EVENT);
        if(appIdentityEdges.hasNext()){
            parentEvent = appIdentityEdges.next().inVertex();
        }

        if(parentEvent == null){
            parentEvent = tx.addVertex();
        }

        for(Vertex platformIdentity : platformIdentities){
            Iterator<Edge> edges = platformIdentity.edges(Direction.OUT, PLATFORM_IDENTITY_PARENT_EVENT);
            if(!edges.hasNext()){
                platformIdentity.addEdge(PLATFORM_IDENTITY_PARENT_EVENT, parentEvent);
            }
        }

        appIdentityEdges = appIdentity.edges(Direction.OUT, APP_IDENTITY_PARENT_EVENT);
        if(!appIdentityEdges.hasNext()){
            appIdentity.addEdge(APP_IDENTITY_PARENT_EVENT, parentEvent);
        }

        tx.commit();

        return parentEvent;
    }


    private Vertex getOrCreateVertex(JanusGraphTransaction tx, Object... objects){
        JanusGraphQuery<? extends JanusGraphQuery> query = tx.query();
        for(int i = 0; i < objects.length; i = i + 2){
            query = query.has((String)objects[i], objects[i + 1]);
        }
        Iterator<JanusGraphVertex> iterator = query.vertices().iterator();
        if(iterator.hasNext()){
            return iterator.next();
        } else {
            return tx.addVertex(objects);
        }
    }

    private Iterator<JanusGraphVertex> queryVertices(JanusGraphTransaction tx, Object... objects) {
        JanusGraphQuery<? extends JanusGraphQuery> query = tx.query();
        for(int i = 0; i < objects.length; i = i + 2){
            query = query.has((String)objects[i], objects[i + 1]);
        }
        return query.vertices().iterator();
    }

    private static class CredentialIdentity {

        final String credType;
        final String credValue;

        private CredentialIdentity(String credType, String credValue) {
            this.credType = credType;
            this.credValue = credValue;
        }
    }

}
