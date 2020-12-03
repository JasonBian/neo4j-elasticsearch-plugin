package com.bianzx.neo4j.elasticsearch;

import static org.neo4j.configuration.SettingValueParsers.BOOL;
import static org.neo4j.configuration.SettingValueParsers.INT;
import static org.neo4j.configuration.SettingValueParsers.STRING;
import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.configuration.Description;
import org.neo4j.configuration.DocumentedDefaultValue;
import org.neo4j.configuration.GroupSetting;
import org.neo4j.graphdb.config.Setting;

/**
 * 
 * @ClassName: ElasticSearchSettings
 * @Description: settings for neo4j elasticsearch plugin
 * @author: bianzexin
 * @date: Nov 2, 2020
 *
 */
@ServiceProvider
public class ElasticSearchSettings extends GroupSetting {

  private ElasticSearchSettings(String dbname) {
    super(dbname);
    if (dbname == null) {
      throw new IllegalArgumentException(
          "ElasticSearchConfig can not be created for scope: " + dbname);
    }
  }

  // For serviceloading
  public ElasticSearchSettings() {
    this("testing");
  }

  public static ElasticSearchSettings forDatabase(String dbname) {
    return new ElasticSearchSettings(dbname);
  }

  public final String PREFIX = "elasticsearch";

  @Description("The host of elasticsearch cluster")
  @DocumentedDefaultValue("http://localhost:9200")
  public final Setting<String> HOST = getBuilder("host", STRING, "http://localhost:9200").build();

  @Description("The elasticsearch index name to store data")
  public final Setting<String> INDEX_NAME = getBuilder("indexName", STRING, "index_default").build();

  @Description("The number of shards of the index, default 5")
  public final Setting<Integer> NUMBER_OF_SHARDS = getBuilder("numberOfShards", INT, 5).build();

  @Description("The number of replicas of the index, default 1")
  public final Setting<Integer> NUMBER_OF_REPLICAS = getBuilder("numberOfReplicas", INT, 1).build();

  @Description("Whether to sync nodes or not, default true")
  @DocumentedDefaultValue("true")
  public final Setting<Boolean> SYNC_NODES = getBuilder("syncNodes", BOOL, Boolean.TRUE).build();

  @Description("Whether to sync relationships or not, default false")
  @DocumentedDefaultValue("false")
  public final Setting<Boolean> SYNC_RELATIONSHIPS = getBuilder("syncRelationships", BOOL, Boolean.FALSE).build();

  @Description("Discover other ElasticSearch cluster node ?")
  @DocumentedDefaultValue("false")
  public final Setting<Boolean> DISCOVERY = getBuilder("discovery", BOOL, Boolean.FALSE).build();

  @Description("Should ElasticSearch indexation use async ?")
  @DocumentedDefaultValue("true")
  public final Setting<Boolean> EXECUTE_ASYNC = getBuilder("executeAsync", BOOL, Boolean.TRUE).build();

  @Override
  public String getPrefix() {
    return PREFIX;
  }
}
