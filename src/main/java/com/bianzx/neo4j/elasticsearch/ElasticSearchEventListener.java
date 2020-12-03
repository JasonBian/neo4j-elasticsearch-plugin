package com.bianzx.neo4j.elasticsearch;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.neo4j.graphdb.Entity;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.event.LabelEntry;
import org.neo4j.graphdb.event.PropertyEntry;
import org.neo4j.graphdb.event.TransactionData;
import org.neo4j.graphdb.event.TransactionEventListener;
import io.searchbox.action.BulkableAction;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestResult;
import io.searchbox.client.JestResultHandler;
import io.searchbox.core.Bulk;
import io.searchbox.core.Delete;
import io.searchbox.core.DocumentResult;
import io.searchbox.core.Index;
import io.searchbox.core.Update;

/**
 * 
 * @ClassName: ElasticSearchEventListener
 * @Description: ElasticSearch Event 监听类
 * @author: bianzexin
 * @date: Nov 2, 2020
 *
 */
public class ElasticSearchEventListener
    implements TransactionEventListener<Collection<BulkableAction<DocumentResult>>>, JestResultHandler<JestResult> {

  private final static Logger logger = Logger.getLogger(ElasticSearchEventListener.class.getName());
  
  //The key of neo4j entity id that stores in elasticsearch.
  private static final String ID = "id";

  // The key of neo4j node labels that stores in elasticsearch.
  private static final String LABELS = "labels";

  // The key of neo4j relationship type that stores in elasticsearch.
  private static final String TYPE = "type";

  // The key of neo4j relationship startNodeId that stores in elasticsearch.
  private static final String START_NODE_ID = "startNodeId";

  // The key of neo4j relationship endNodeId that stores in elasticsearch.
  private static final String END_NODE_ID = "endNodeId";

  // The key of neo4j entity properties that stores in elasticsearch.
  private static final String PROPERTIES = "properties";
  
  // Since ElasticSearch 6.x, there was not support multiple types for one index
  private static final String INDEX_TYPE = "Neo4jIndex";

  // The client for ElasticSearch
  private final JestClient jestClient;

  // The index Name for ElasticSearch Index
  private final String indexName;

  // Whether to sync nodes or not
  private final boolean syncNodes;

  // Whether to sync relationships or not
  private final boolean syncRelationships;

  // Should ElasticSearch indexation use async
  private final boolean executeAsync;
  
  private final String indexType;

  /**
   * Construct for ElasticSearchEventListener
   * @param builder
   */
  private ElasticSearchEventListener(Builder builder) {
    this.jestClient = builder.jestClient;
    this.indexName = builder.indexName;
    this.syncNodes = builder.syncNodes;
    this.syncRelationships = builder.syncRelationships;
    this.executeAsync = builder.executeAsync;
    this.indexType = INDEX_TYPE;
  }

  /**
   * ####################################################
   * # Implements methods for {@link JestResultHandler} #
   * ####################################################
   */
  @Override
  public void completed(JestResult result) {
    if (result.isSucceeded() && result.getErrorMessage() == null) {
      logger.fine("data transfer completed,jsonData:" + result.getJsonString());
    } else {
      logger.severe("data transfer error: " + result.getErrorMessage() + ",jsonData:" + result.getJsonString());
    }
  }

  @Override
  public void failed(Exception ex) {
    logger.log(Level.WARNING, "data transfer failed", ex);
  }
  
  /**
   * ###########################################################
   * # Implements methods for {@link TransactionEventListener} #
   * ###########################################################
   */
  @Override
  public Collection<BulkableAction<DocumentResult>> beforeCommit(TransactionData data, Transaction transaction,
      GraphDatabaseService databaseService) throws Exception {
    Map<SyncDataKey, BulkableAction<DocumentResult>> actions = new LinkedHashMap<>();
    if (syncNodes) {
      // all changed nodes
      logger.severe("Starting to sync indexing Nodes");
      collectChangedNodes(actions, data);
    }
    if (syncRelationships) {
      // all changed relationships
      logger.severe("Starting to sync indexing Relationships");
      collectChangedRelations(actions, data);
    }
    return actions.isEmpty() ? Collections.<BulkableAction<DocumentResult>>emptyList() : actions.values();
  }

  @Override
  public void afterCommit(TransactionData data, Collection<BulkableAction<DocumentResult>> state,
      GraphDatabaseService databaseService) {
    if (state.isEmpty()) {
      return;
    }
    try {
      Bulk bulk = new Bulk.Builder().addAction(state).build();
      if (executeAsync) {
        jestClient.executeAsync(bulk, this);
      } else {
        jestClient.execute(bulk);
      }
    } catch (Exception e) {
      logger.log(Level.SEVERE, "data transfer execution error after commit", e);
    }
  }

  @Override
  public void afterRollback(TransactionData data, Collection<BulkableAction<DocumentResult>> state,
      GraphDatabaseService databaseService) {
  }

  /**
   * sync nodes datas from Neo4j to ElasticSearch
   * @param actions
   * @param data
   */
  private void collectChangedNodes(Map<SyncDataKey, BulkableAction<DocumentResult>> actions,
      TransactionData data) {
    // created nodes
    for (Node node : data.createdNodes()) {
      String id = id(node);
      actions.put(new SyncDataKey(indexName, indexType, id), indexRequest(id, node));
    }

    // deleted nodes
    for (Node node : data.deletedNodes()) {
      String id = id(node);
      actions.put(new SyncDataKey(indexName, indexType, id), deleteRequest(id, node));
    }

    // assigned labels
    for (LabelEntry labelEntry : data.assignedLabels()) {
      Node node = labelEntry.node();
      String id = id(node);
      if (data.isDeleted(labelEntry.node())) {
        actions.put(new SyncDataKey(indexName, indexType, id), deleteRequest(id, node));
      } else {
        actions.put(new SyncDataKey(indexName, indexType, id), indexRequest(id, labelEntry.node()));
      }
    }

    // removed labels
    for (LabelEntry labelEntry : data.removedLabels()) {
      Node node = labelEntry.node();
      String id = id(node);
      actions.put(new SyncDataKey(indexName, indexType, id), deleteRequest(id, node));
    }

    // assigned node properties
    for (PropertyEntry<Node> propEntry : data.assignedNodeProperties()) {
      Node node = propEntry.entity();
      String id = id(node);
      actions.put(new SyncDataKey(indexName, indexType, id), indexRequest(id, node));
    }

    // removed node properties
    for (PropertyEntry<Node> propEntry : data.removedNodeProperties()) {
      Node node = propEntry.entity();
      String id = id(node);
      if (data.isDeleted(node)) {
        actions.put(new SyncDataKey(indexName, indexType, id), deleteRequest(id, node));
      } else {
        actions.put(new SyncDataKey(indexName, indexType, id), indexRequest(id, node));
      }
    }
  }

  /**
   * sync relationships data from neo4j to ElasticSearch
   * @param actions
   * @param data
   */
  private void collectChangedRelations(Map<SyncDataKey, BulkableAction<DocumentResult>> actions,
      TransactionData data) {
    // created relationships
    for (Relationship relationship : data.createdRelationships()) {
      String id = id(relationship);
      logger.severe("Relationship id is:" + id);
      actions.put(new SyncDataKey(indexName, indexType, id), indexRequest(id, relationship));
    }

    // deleted relationships
    for (Relationship relationship : data.deletedRelationships()) {
      String id = id(relationship);
      actions.put(new SyncDataKey(indexName, indexType, id), deleteRequest(id, relationship));
    }

    // assigned relationship properties
    for (PropertyEntry<Relationship> propEntry : data.assignedRelationshipProperties()) {
      Relationship relationship = propEntry.entity();
      String id = id(relationship);
      actions.put(new SyncDataKey(indexName, indexType, id), indexRequest(id, relationship));
    }

    // removed relationship properties
    for (PropertyEntry<Relationship> propEntry : data.removedRelationshipProperties()) {
      Relationship relationship = propEntry.entity();
      String id = id(relationship);
      if (data.isDeleted(relationship)) {
        actions.put(new SyncDataKey(indexName, indexType, id), deleteRequest(id, relationship));
      } else {
        actions.put(new SyncDataKey(indexName, indexType, id), indexRequest(id, relationship));
      }
    }
  }

  /**
   * construct indexRequest
   * @param id
   * @param entity
   * @return
   */
  private BulkableAction<DocumentResult> indexRequest(String id, Entity entity) {
    Index.Builder builder = new Index.Builder(properties(id, entity)).index(indexName).id(id);
    return builder.type(indexType).build();
  }

  /**
   * construct deleteRequest
   * @param id
   * @param entity
   * @return
   */
  private BulkableAction<DocumentResult> deleteRequest(String id, Entity entity) {
    Delete.Builder builder = new Delete.Builder(id).index(indexName);
    return builder.type(indexType).build();
  }

  /**
   * construct updateRequest
   * @param id
   * @param entity
   * @return
   */
  private BulkableAction<DocumentResult> updateRequest(String id, Entity entity) {
    Update.Builder builder = new Update.Builder(properties(id, entity)).index(indexName).id(id);
    return builder.type(indexType).build();
  }

  /**
   * get Id of Entity
   * @param entity
   * @return
   */
  private String id(Entity entity) {
    return String.valueOf(entity.getId());
  }

  /**
   * construct properties for Entity
   * @param id
   * @param entity
   * @return
   */
  private Map<String, Object> properties(String id, Entity entity) {
    Map<String, Object> props = new LinkedHashMap<>();
    props.put(ID, id);
    props.put(PROPERTIES, entity.getAllProperties());
    if (entity instanceof Node) {
      // node labels
      props.put(LABELS, labels(((Node) entity).getLabels()));
    } else if (entity instanceof Relationship) {
      // relationship type
      props.put(TYPE, ((Relationship) entity).getType().name());
      // relationship startNodeId
      props.put(START_NODE_ID, String.valueOf(((Relationship) entity).getStartNodeId()));
      // relationship endNodeId
      props.put(END_NODE_ID, String.valueOf(((Relationship) entity).getEndNodeId()));
    }
    return props;
  }

  /**
   * get labels for Node
   * Neo4j Node allows multiple labels
   * @param labels
   * @return
   */
  private List<String> labels(Iterable<Label> labels) {
    List<String> list = new LinkedList<>();
    for (Label label : labels) {
      list.add(label.name());
    }
    return list;
  }

  /**
   * The key of the map which stores changed data before commit. This is used to avoid duplicate
   * data transformation. Only data with different index, type and id can be transferred to
   * elasticsearch.
   */
  private static class SyncDataKey {

    private final String index;

    private final String type;

    private final String id;

    public SyncDataKey(String index, String type, String id) {
      this.index = index;
      this.type = type;
      this.id = id;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      SyncDataKey that = (SyncDataKey) o;
      if (!index.equals(that.index)) {
        return false;
      }
      if (!type.equals(that.type)) {
        return false;
      }
      return id.equals(that.id);
    }

    @Override
    public int hashCode() {
      int result = index.hashCode();
      result = 31 * result + type.hashCode();
      result = 31 * result + ((id == null) ? 0 : id.hashCode());
      return result;
    }
  }

  /**
   * 
   * @ClassName: Builder
   * @Description: build ElasticSearchEventListener
   * @author: bianzexin
   * @date: Nov 2, 2020
   *
   */
  public static class Builder {

    private JestClient jestClient;

    private String indexName;

    private boolean syncNodes;

    private boolean syncRelationships;

    private boolean executeAsync;

    public Builder() {}

    public Builder jestClient(JestClient jestClient) {
      this.jestClient = jestClient;
      return this;
    }

    public Builder indexName(String indexName) {
      this.indexName = indexName;
      return this;
    }

    public Builder syncNodes(boolean syncNodes) {
      this.syncNodes = syncNodes;
      return this;
    }

    public Builder syncRelationships(boolean syncRelationships) {
      this.syncRelationships = syncRelationships;
      return this;
    }

    public Builder executeAsync(boolean executeAsync) {
      this.executeAsync = executeAsync;
      return this;
    }

    public ElasticSearchEventListener build() {
      return new ElasticSearchEventListener(this);
    }
  }
}
