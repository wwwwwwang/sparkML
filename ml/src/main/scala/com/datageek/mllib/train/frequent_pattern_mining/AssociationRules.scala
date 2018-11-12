package com.datageek.mllib.train.frequent_pattern_mining

import org.apache.commons.logging.LogFactory
import org.apache.spark.mllib.fpm.AssociationRules
import org.apache.spark.mllib.fpm.FPGrowth.FreqItemset
import org.apache.spark.{SparkConf, SparkContext}

object AssociationRules {
  val log = LogFactory.getLog(AssociationRules.getClass)

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("AssociationRules")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)

    val freqItemsets = sc.parallelize(Seq(
      new FreqItemset(Array("a"), 15L),
      new FreqItemset(Array("b"), 35L),
      new FreqItemset(Array("a", "b"), 12L)
    ))

    val ar = new AssociationRules()
      .setMinConfidence(0.8)
    val results = ar.run(freqItemsets)

    results.collect().foreach { rule =>
      println("[" + rule.antecedent.mkString(",")
        + "=>"
        + rule.consequent.mkString(",") + "]," + rule.confidence)
    }
    sc.stop()
  }
}
