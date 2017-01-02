package org.koeninger.spark.citus

/** something that can be used as a distribution key for Citus partitioning */
trait CitusKey {
  /** name of the table (without shard number) */
  def table: String

  /** the distribution key, aka value taken from the distribution column */
  def key: Int
}
