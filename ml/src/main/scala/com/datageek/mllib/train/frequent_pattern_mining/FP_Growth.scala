package com.datageek.mllib.train.frequent_pattern_mining

import com.datageek.util.MysqlJDBCDao
import org.apache.commons.logging.LogFactory
import org.apache.spark.mllib.fpm.FPGrowth
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

object FP_Growth {
  val log = LogFactory.getLog(FP_Growth.getClass)

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("FP_Growth")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)

    val configName = args(0)

    var path = ""
    var sql = ""
    var minSupport = 0.5
    var numPartitions = 3
    var minConfidence = 0.0
    var savePath = ""

    val myjdbc = new MysqlJDBCDao()
    val configs = myjdbc.getConfig(configName)
    if (configs.containsKey("path")) {
      path = configs.get("path")
    }
    if (configs.containsKey("minsupport")) {
      minSupport = configs.get("minsupport").toDouble
    }
    if (configs.containsKey("numpartitions")) {
      numPartitions = configs.get("numpartitions").toInt
    }
    if (configs.containsKey("minconfidence")) {
      minConfidence = configs.get("minconfidence").toDouble
    }
    if (configs.containsKey("sql") && configs.get("sql") != "") {
      sql = configs.get("sql")
    }
    if (configs.containsKey("savepath") && configs.get("savepath") != "") {
      savePath = configs.get("savepath")
      savePath += "/" + System.currentTimeMillis()
    }

    log.info("#####################path =" + path)
    log.info("#####################minConfidence =" + minConfidence)
    log.info("#####################numPartitions =" + numPartitions)
    log.info(s"#####################minSupport = $minSupport")
    log.info(s"#####################sql = $sql")
    log.info("#####################savePath =" + savePath)
    myjdbc.closeConn()

    var data: RDD[String] = null
    if (!path.equalsIgnoreCase("") && path != null) {
      data = sc.textFile(path)
    } else if (!sql.equalsIgnoreCase("") && sql != null) {
      val hContext = new HiveContext(sc)
      data = hContext.sql(sql).rdd.map(_.mkString(" "))
      //hContext.sql(sql).map(r => {val a = r.toString.split(",")
      //LabeledPoint(a.last.toDouble, Vectors.dense(a.take(a.length-1).map(_.toDouble)))})
    } else {
      log.info(s"Nothing can be used as training data, both path and sql are not set")
      System.exit(1)
    }
    //val data = sc.textFile(path) //"data/mllib/sample_fpgrowth.txt"

    val transactions: RDD[Array[String]] = data.map(s => s.trim.split(' '))

    val fpg = new FPGrowth().setMinSupport(minSupport).setNumPartitions(numPartitions)
    val model = fpg.run(transactions)
    model.freqItemsets.collect().foreach { itemset =>
      println(itemset.items.mkString("[", ",", "]") + ", " + itemset.freq)
    }
    if (minConfidence != 0.0) {
      model.generateAssociationRules(minConfidence).collect().foreach { rule =>
        println(
          rule.antecedent.mkString("[", ",", "]")
            + " => " + rule.consequent.mkString("[", ",", "]")
            + ", " + rule.confidence)
      }
    }
    sc.stop()
  }
}
