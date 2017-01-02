package org.koeninger.spark.citus

/** container of info for an individual shard placement */
case class ShardPlacement(
    tableName: String,
    shardMaxValue: Int,
    shardId: Int,
    nodeName: String,
    nodePort: Int) {
  def shardTableName: String = tableName + "_" + shardId
}
