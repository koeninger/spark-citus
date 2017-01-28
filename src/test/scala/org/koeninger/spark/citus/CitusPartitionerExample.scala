package org.koeninger.spark.citus

import org.apache.spark._
import org.apache.spark.SparkContext._
import scala.util.control.NonFatal
import scala.util.Random
import scalikejdbc._

/** 
 * Example usage of CitusPartitioner.
 * You need a running citus cluster, see https://docs.citusdata.com/en/v6.0/installation/development.html
 * This is quick and dirty for illustrative purposes,
 * you'd want to replace hard-coded configuration (e.g. databaseName) in various places.
 * Run via sbt 'test:runMain org.koeninger.spark.citus.CitusPartitionerExample'
 */
object CitusPartitionerExample {
  val citusMaster = "localhost:9700"

  def main(args: Array[String]): Unit = {
    initJdbc(citusMaster)
    createTable(citusMaster)

    val partitioner = NamedDB(citusMaster).localTx { implicit session =>
      CitusPartitioner(List("sometable"))
    }

    // some super interesting data to work with
    val data = (1 to 1000).map { i =>
      (Random.nextInt, Random.nextLong)
    }

    val sc = new SparkContext("local[4]", "ExampleCitus")

    sc.parallelize(data).map {
      case (id, value) => (("sometable", id), value)
    }.partitionBy(partitioner).foreachPartition { iter =>
      // use the partitioner to lookup corect citus worker shard
      val shard = partitioner.shardPlacement(TaskContext.get.partitionId)
      val hostAndPort = shard.nodeName + ":" + shard.nodePort

      // make sure connection pool is started on spark executor
      initJdbc(hostAndPort)

      val rows = iter.map {
        case ((table, id), value) => Seq(id, value)
      }.toSeq

      // do inserts directly to citus worker shard
      NamedDB(hostAndPort).localTx { implicit session =>
        SQL(s"insert into ${shard.shardTableName} (id, value) values (?, ?)").
          batch(rows: _*).apply()
      }
    }

    sc.stop()

    // make sure results got written to correct shard and are queryable from master
    NamedDB(citusMaster).localTx { implicit session =>
      data.foreach {
        case (id, value) =>
          val count = sql"select count(*) from sometable where id = $id and value = $value".
            map(rs => rs.long(1)).first.apply()
          assert(count.isDefined && count.get >= 1, s"didn't insert $id $value correctly")
      }
    }
  }

  // track which connection pools have been established
  private val connected = scala.collection.mutable.Set[String]()

  /** expects "host:port", adds a connection pool with that name */
  def initJdbc(hostAndPort: String): Unit = connected.synchronized {
    if (!connected(hostAndPort)) {
      val driver = "org.postgresql.Driver"
      val databaseName = "cody"
      val user = "cody"
      val password = ""
      val extraParams = "stringtype=unspecified"
      val url = s"jdbc:postgresql://$hostAndPort/$databaseName?$extraParams"

      Class.forName(driver)
      ConnectionPool.add(hostAndPort, url, user, password)
      connected.add(hostAndPort)
    }
  }

  /** create a distributed table to hold results */
  def createTable(db: String): Unit =
    NamedDB(db).localTx { implicit session =>
      try {
        sql"""
create table if not exists sometable(
  id integer,
  value bigint
)
""".execute.apply()
        sql"set citus.shard_replication_factor = 1".execute.apply()
        sql"set citus.shard_count = 4".execute.apply()
        sql"select create_distributed_table('sometable', 'id')".execute.apply()
      } catch {
        case NonFatal(x) => System.err.println(x)
      }
    }
}
