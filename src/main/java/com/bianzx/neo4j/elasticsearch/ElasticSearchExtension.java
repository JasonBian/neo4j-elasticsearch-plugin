package com.bianzx.neo4j.elasticsearch;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.neo4j.configuration.Config;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;
import org.neo4j.logging.internal.LogService;
import org.neo4j.procedure.impl.GlobalProceduresRegistry;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestResult;
import io.searchbox.indices.CreateIndex;
import io.searchbox.indices.IndicesExists;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;

/**
 * 
 * @ClassName: ElasticSearchExtension
 * @Description: ElasticSearch Extension实现Neo4j和ElasticSearch之间的数据同步
 * @author: bianzexin
 * @date: Nov 2, 2020
 *
 */
public class ElasticSearchExtension extends LifecycleAdapter {
  
  /**
   * ####################################
   * # Config params for Neo4j Plugins  #
   * ####################################
   */
  private final Log logger;

  private final LogService log;
  
  private final Config config;
  
  private final GraphDatabaseAPI db;
  
  private final DatabaseManagementService dbms;
  
  private final GlobalProceduresRegistry globalProceduresRegistry;


  /**
   * ####################################
   * # Config params for ElasticSearch  #
   * ####################################
   */
  private  String host;

  private  String indexName;

  private  Integer numberOfShards;

  private  Integer numberOfReplicas;

  private  Boolean syncNodes;

  private  Boolean syncRelationships;

  private  Boolean executeAsync;

  private  Boolean discovery;
  
  
  private JestClient jestClient;

  private ElasticSearchEventListener elasticSearchEventListener;
  

  /**
   * expose this  instance via `@Context ElasticSearchExtension config`
   * @param log
   * @param config
   * @param db
   * @param dbms
   * @param globalProceduresRegistry
   */
  public ElasticSearchExtension(LogService log, Config config, GraphDatabaseAPI db,
      DatabaseManagementService dbms, GlobalProceduresRegistry globalProceduresRegistry) {
    logger = log.getUserLog(ElasticSearchExtension.class);
    logger.info(String.format("[%s] Creating ElasticSearchExtension", db.databaseName()));
    
    this.log = log;
    this.config = config;
    this.db = db;
    this.dbms = dbms;
    this.globalProceduresRegistry = globalProceduresRegistry;
    globalProceduresRegistry.registerComponent((Class<ElasticSearchExtension>) getClass(), ctx -> this, true);
    logger.error("register component for ElasticSearchExtension");
  }


  @Override
  public void init() {
    if (!SYSTEM_DATABASE_NAME.equalsIgnoreCase(db.databaseName())) {
      logger.info(String.format("[%s] Starting ElasticSearchExtension", db.databaseName()));
      
      //init ElasticSearch config
      ElasticSearchSettings esConfig =
          config.getGroups(ElasticSearchSettings.class).get(db.databaseName());
      host = config.get(esConfig.HOST);
      indexName = config.get(esConfig.INDEX_NAME);
      numberOfShards = config.get(esConfig.NUMBER_OF_SHARDS);
      numberOfReplicas = config.get(esConfig.NUMBER_OF_REPLICAS);
      syncNodes = config.get(esConfig.SYNC_NODES);
      syncRelationships = config.get(esConfig.SYNC_RELATIONSHIPS);
      executeAsync = config.get(esConfig.EXECUTE_ASYNC);
      discovery = config.get(esConfig.DISCOVERY);
      
      // get JestClient
      try {
        jestClient = JestHttpClientFactory.getClient(host, discovery);
      } catch (Throwable e1) {
        logger.info("get elsticsearch client error:" + e1.getMessage());
      }
      // whether the specific index name exists
      try {
        if (existsIndex(indexName)) {
          logger.info("ElasticSearch Index: [" + indexName + "] already exists.");
        } else {
          // create specific index
          Map<String, Object> settings = new HashMap<>(4);
          settings.put("number_of_shards", numberOfShards);
          settings.put("number_of_replicas", numberOfReplicas);
          if (createIndex(indexName, settings)) {
            logger.info("ElasticSearch Index: [" + indexName + "] created.");
          } else {
            logger.info("ElasticSearch Index: [" + indexName + "] create failed.");
            return;
          }
        }
      } catch (IOException e) {
        logger.info("Init elasticsearch index error," + e.getMessage());
      }

      // build ElasticSearchEventHandler
      elasticSearchEventListener = new ElasticSearchEventListener.Builder().jestClient(jestClient)
          .indexName(indexName).syncNodes(syncNodes).syncRelationships(syncRelationships)
          .executeAsync(executeAsync).build();


      // register ElasticSearchEventHandler to GraphDatabaseService
      dbms.registerTransactionEventListener(db.databaseName(), elasticSearchEventListener);
      logger.info("Neo4j elasticsearch plugin registered!");
    }
  }

  @Override
  public void shutdown() throws IOException {
    if (!SYSTEM_DATABASE_NAME.equalsIgnoreCase(db.databaseName())) {
      logger.info(String.format("[%s] Stopping ElasticSearch Extension", db.databaseName()));
      if (elasticSearchEventListener != null) {
        this.dbms.unregisterTransactionEventListener(db.databaseName(), elasticSearchEventListener);
      }
      if (jestClient != null) {
        this.jestClient.close();
      }
      logger.info("Neo4j elasticsearch Extension shutdown!");
    }
  }

  /**
   * 判断ElasticSearch index是否存在
   * @param indexName
   * @return
   * @throws IOException
   */
  private boolean existsIndex(String indexName) throws IOException {
    JestResult result = jestClient.execute(new IndicesExists.Builder(indexName).build());
    return result.isSucceeded();
  }

  /**
   * 创建ElasticSearch index
   * @param indexName
   * @param settings
   * @return
   * @throws IOException
   */
  private boolean createIndex(String indexName, Map<String, Object> settings) throws IOException {
    final JestResult result =
        jestClient.execute(new CreateIndex.Builder(indexName).settings(settings).build());
    return result.isSucceeded();
  }
}
