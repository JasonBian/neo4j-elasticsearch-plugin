package com.bianzx.neo4j.elasticsearch;

import org.neo4j.configuration.Config;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.kernel.extension.ExtensionFactory;
import org.neo4j.kernel.extension.ExtensionType;
import org.neo4j.kernel.extension.context.ExtensionContext;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.logging.Log;
import org.neo4j.logging.internal.LogService;
import org.neo4j.procedure.impl.GlobalProceduresRegistry;

/**
 * 
 * @ClassName: ElasticSearchKernelExtensionFactory
 * @Description: ElasticSearch Kernel Extension Factory
 * @author: bianzexin
 * @date: Nov 2, 2020
 *
 */
public class ElasticSearchKernelExtensionFactory extends ExtensionFactory<ElasticSearchKernelExtensionFactory.Dependencies>{

  private static final String SERVICE_NAME = "ELASTIC_SEARCH";

  public ElasticSearchKernelExtensionFactory() {
      super(ExtensionType.DATABASE, SERVICE_NAME);
  }

  @Override
  public Lifecycle newInstance(ExtensionContext context, Dependencies dependencies) {

    //create the extension
    GraphDatabaseAPI db = dependencies.graphdatabaseAPI();
    LogService log = dependencies.log();
    DatabaseManagementService dbms = dependencies.databaseManagementService();
    GlobalProceduresRegistry globalProceduresRegistry = dependencies.globalProceduresRegistry();
    Config config = dependencies.config();
    
    Log logger = log.getUserLog(ElasticSearchKernelExtensionFactory.class);
    logger.info(String.format("[%s] Create instance ExtensionFactory", db.databaseName()));

    return new ElasticSearchExtension(log, config, db, dbms, globalProceduresRegistry);

  }

  /**
   * ##############################################################
   * ## Dependencies for creating new kernel extension instance. ##
   * ##############################################################
   */
  public interface Dependencies {

    Config config();

    DatabaseManagementService databaseManagementService();

    GlobalProceduresRegistry globalProceduresRegistry();

    GraphDatabaseAPI graphdatabaseAPI();

    LogService log();
  }
}
