package com.datageek.mllib.train.frequent_pattern_mining

import org.apache.commons.logging.LogFactory
import org.apache.spark.mllib.fpm.PrefixSpan
import org.apache.spark.{SparkConf, SparkContext}

object PrefixSpan {
  val log = LogFactory.getLog(PrefixSpan.getClass)

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("PrefixSpan")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)

    val sequences = sc.parallelize(Seq(
      Array(Array(1, 2), Array(3)),
      Array(Array(1), Array(3, 2), Array(1, 2)),
      Array(Array(1, 2), Array(5)),
      Array(Array(6))
    ), 2).cache()
    val prefixSpan = new PrefixSpan()
      .setMinSupport(0.5)
      .setMaxPatternLength(5)
    val model = prefixSpan.run(sequences)
    model.freqSequences.collect().foreach { freqSequence =>
      println(
        freqSequence.sequence.map(_.mkString("[", ", ", "]")).mkString("[", ", ", "]") +
          ", " + freqSequence.freq)
    }
    sc.stop()
  }
}
