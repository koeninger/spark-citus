package org.koeninger.spark.citus

import org.scalatest.FunSuite

class CitusPartitionerSuite extends FunSuite {
  test("creation") {
    // out of order
    intercept[AssertionError] {
      new CitusPartitioner(Array(
        ShardPlacement("test", -715827884, 104318, "ip-XX.ec2.internal", 5432),
        ShardPlacement("test", 2147483647, 104320, "ip-YY.ec2.internal", 5432),
        ShardPlacement("test", 715827881, 104319, "ip-ZZ.ec2.internal", 5432)
      ))
    }

    // too many placements
    intercept[AssertionError] {
      new CitusPartitioner(Array(
        ShardPlacement("test", -715827884, 104318, "ip-XX.ec2.internal", 5432),
        ShardPlacement("test", 2147483647, 104320, "ip-YY.ec2.internal", 5432),
        ShardPlacement("test", 715827881, 104319, "ip-ZZ.ec2.internal", 5432),
        ShardPlacement("test", -715827884, 104318, "ip-XX.ec2.internal", 5432),
        ShardPlacement("test", 2147483647, 104320, "ip-YY.ec2.internal", 5432),
        ShardPlacement("test", 715827881, 104319, "ip-ZZ.ec2.internal", 5432)
      ))
    }

    // tables with different bucket schemes
    intercept[AssertionError] {
      new CitusPartitioner(Array(
        ShardPlacement("test", -715827884, 104318, "ip-XX.ec2.internal", 5432),
        ShardPlacement("test", 2147483647, 104320, "ip-YY.ec2.internal", 5432),
        ShardPlacement("test", 715827881, 104319, "ip-ZZ.ec2.internal", 5432),
        ShardPlacement("testbad", -1073741825, 104321, "ip-XX.ec2.internal", 5432),
        ShardPlacement("testbad", -1, 104322, "ip-ZZ.ec2.internal", 5432),
        ShardPlacement("testbad", 1073741823, 104323, "ip-YY.ec2.internal", 5432),
        ShardPlacement("testbad", 2147483647, 104324, "ip-XX.ec2.internal", 5432)
      ))
    }

    val ok = new CitusPartitioner(Array(
      ShardPlacement("test", -715827884, 104318, "ip-XX.ec2.internal", 5432),
      ShardPlacement("test", 715827881, 104319, "ip-ZZ.ec2.internal", 5432),
      ShardPlacement("test", 2147483647, 104320, "ip-YY.ec2.internal", 5432),
      ShardPlacement("test2", -715827884, 104318, "ip-XX.ec2.internal", 5432),
      ShardPlacement("test2", 715827881, 104319, "ip-ZZ.ec2.internal", 5432),
      ShardPlacement("test2", 2147483647, 104320, "ip-YY.ec2.internal", 5432)
    ))

    assert(ok.numPartitions == 6)
  }

  test("find bucket") {
    val placements = Array(
      ShardPlacement("test", -715827884, 104318, "ip-XX.ec2.internal", 5432),
      ShardPlacement("test", 715827881, 104319, "ip-ZZ.ec2.internal", 5432),
      ShardPlacement("test", 2147483647, 104320, "ip-YY.ec2.internal", 5432)
    )

    val part = new CitusPartitioner(placements)

    assert(part.findBucket(Int.MinValue) == 0)
    assert(part.findBucket(-715827884) == 0)
    assert(part.findBucket(-715827883) == 1)
    assert(part.findBucket(-1) == 1)
    assert(part.findBucket(0) == 1)
    assert(part.findBucket(1) == 1)
    assert(part.findBucket(715827881) == 1)
    assert(part.findBucket(715827882) == 2)
    assert(part.findBucket(Int.MaxValue) == 2)
  }

  test("find partition") {
    val ok = new CitusPartitioner(Array(
      ShardPlacement("test", -715827884, 104325, "ip-XX.ec2.internal", 5432),
      ShardPlacement("test", 715827881, 104326, "ip-ZZ.ec2.internal", 5432),
      ShardPlacement("test", 2147483647, 104327, "ip-YY.ec2.internal", 5432),
      ShardPlacement("test2", -715827884, 104328, "ip-XX.ec2.internal", 5432),
      ShardPlacement("test2", 715827881, 104329, "ip-ZZ.ec2.internal", 5432),
      ShardPlacement("test2", 2147483647, 104330, "ip-YY.ec2.internal", 5432)
    ))

    // These may seem like magic numbers, but were obtained from actually creating matching citus tables and inserting into them
    assert(0 == ok.findPartition("test", 1))
    assert(0 == ok.findPartition("test", Int.MinValue))
    assert(1 == ok.findPartition("test", -1))
    assert(1 == ok.findPartition("test", 0))
    assert(1 == ok.findPartition("test", Int.MaxValue))
    assert(2 == ok.findPartition("test", 23))
    assert(2 == ok.findPartition("test", 42))
    assert(3 == ok.findPartition("test2", 1))
    assert(3 == ok.findPartition("test2", Int.MinValue))
    assert(4 == ok.findPartition("test2", -1))
    assert(4 == ok.findPartition("test2", 0))
    assert(4 == ok.findPartition("test2", Int.MaxValue))
    assert(5 == ok.findPartition("test2", 23))
    assert(5 == ok.findPartition("test2", 42))

    // make sure conversion from tuple works for spark getPartition
    assert(5 == ok.getPartition(("test2", 42)))

    class MyCitusKey(val key: Int) extends CitusKey {
      def table = "test2"
    }

    // make sure conversion from CitusKey works for spark getPartition
    assert(5 == ok.getPartition(new MyCitusKey(42)))
  }
}
