package com.datageek.mllib.train.clustering

import com.datageek.util.MysqlJDBCDao
import org.apache.commons.logging.LogFactory
import org.apache.spark.mllib.clustering.LDA
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

object LatentDirichletAllocation {
  val log = LogFactory.getLog(LatentDirichletAllocation.getClass)

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("LatentDirichletAllocation")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)

    val configName = args(0)

    var path = ""
    var sql = ""
    var hasLabel = false
    var numClusters = 3
    var savePath = ""

    val myjdbc = new MysqlJDBCDao()
    val configs = myjdbc.getConfig(configName)
    if (configs.containsKey("path")) {
      path = configs.get("path")
    }
    if (configs.containsKey("sql")) {
      sql = configs.get("sql")
    }
    if (configs.containsKey("haslabel")) {
      hasLabel = configs.get("haslabel").toBoolean
    }
    if (configs.containsKey("numclusters")) {
      numClusters = configs.get("numclusters").toInt
    }
    if (configs.containsKey("savepath")) {
      savePath = configs.get("savepath")
      savePath += "/" + System.currentTimeMillis()
    }

    log.info(s"#####################path = $path")
    log.info(s"#####################sql = $sql")
    log.info(s"#####################hasLabel = $hasLabel")
    log.info(s"#####################numClusters = $numClusters")
    log.info(s"#####################savePath = $savePath")
    myjdbc.closeConn()

    var data: RDD[String] = null
    if (!path.equalsIgnoreCase("") && path != null) {
      data = sc.textFile(path)
    } else if (!sql.equalsIgnoreCase("") && sql != null) {
      val hContext = new HiveContext(sc)
      data = hContext.sql(sql).rdd.map(_.mkString(" "))
    } else {
      log.info(s"Nothing can be used as training data, both path and sql are not set")
      System.exit(1)
    }
    val parsedData = data.map(s =>
      Vectors.dense(if (hasLabel) s.trim.split(' ').tail.map(_.toDouble)
      else s.trim.split(' ').map(_.toDouble))).cache()

    val corpus = parsedData.zipWithIndex.map(_.swap).cache()

    val ldaModel = new LDA().setK(numClusters).run(corpus)

    // Output topics. Each is a distribution over words (matching word count vectors)
    println("Learned topics (as distributions over vocab of " + ldaModel.vocabSize + " words):")
    val topics = ldaModel.topicsMatrix
    for (topic <- Range(0, numClusters)) {
      print("Topic " + topic + ":")
      for (word <- Range(0, ldaModel.vocabSize)) {
        print(" " + topics(word, topic))
      }
      println()
    }

    if (!savePath.equalsIgnoreCase("") && savePath != null) {
      ldaModel.save(sc, savePath)
    } else {
      log.info(s"savePath is empty, no needing to save....")
    }
    //val sameModel = DistributedLDAModel.load(sc,"target/org/apache/spark/LatentDirichletAllocationExample/LDAModel")
    sc.stop()
  }
}
