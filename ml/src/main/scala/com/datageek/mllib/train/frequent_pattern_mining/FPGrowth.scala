package com.datageek.mllib.train.frequent_pattern_mining

import java.util.Properties

import com.datageek.util.{FPGItems, FPGRules, MysqlJDBCDao, TrainResult}
import org.apache.commons.logging.{Log, LogFactory}
import org.apache.spark.mllib.fpm.FPGrowth
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.{ArrayBuffer, ListBuffer}

object FPGrowth {
  val log: Log = LogFactory.getLog(FPGrowth.getClass)

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("FP_Growth")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.registerKryoClasses(
      Array(classOf[ArrayBuffer[String]], classOf[ListBuffer[String]])
    )
    val sc = new SparkContext(conf)
    val sqlContext = new HiveContext(sc)
    import sqlContext.implicits._

    val arg = args(0)
    val Array(configName, jobID) = arg.split("@2@")

    var path = ""
    var sql = ""
    var minSupport = "0.3"
    var numPartitions = -1
    var minConfidence = 0.8
    var outmode = "Items"
    //Items:计算输出出现频率最高的项集；Rules: 计算输出可信度较
    var savePath = ""
    var hiveTable = ""

    var tmpjobid = jobID
    if (jobID.contains(","))
      tmpjobid = jobID.split(",")(0)

    val myjdbc = new MysqlJDBCDao()
    val configs = myjdbc.getConfig(tmpjobid)

    if (configs.containsKey("data")) {
      sql = configs.get("data")
    }
    if (configs.containsKey("path")) {
      path = configs.get("path")
    }
    if (configs.containsKey("minsupport")) {
      minSupport = configs.get("minsupport")
    }
    if (configs.containsKey("output")) {
      outmode = configs.get("output")
    }
    if (configs.containsKey("numpartitions")) {
      numPartitions = configs.get("numpartitions").toInt
    }
    if (configs.containsKey("minconfidence")) {
      minConfidence = configs.get("minconfidence").toDouble
    }
    if (configs.containsKey("saveresult")) {
      hiveTable = configs.get("saveresult")
    }
    if (configs.containsKey("savemodel")) {
      savePath = configs.get("savemodel")
    }

    log.info("#####################path =" + path)
    log.info("#####################minConfidence =" + minConfidence)
    log.info("#####################numPartitions =" + numPartitions)
    log.info(s"#####################minSupport = $minSupport")
    log.info(s"#####################sql = $sql")
    log.info(s"#####################outmode = $outmode")
    log.info(s"#####################savePath = $savePath")
    log.info(s"#####################hiveTable = $hiveTable")

    val connectionProperties = new Properties
    connectionProperties.put("user", myjdbc.dbUser)
    connectionProperties.put("password", myjdbc.dbPassword)
    val url = myjdbc.dbUrl
    val DbName = myjdbc.getDbNameInDictionary
    myjdbc.updateMLTrainAppId(tmpjobid, sc.applicationId)
    myjdbc.closeConn()

    if (!sql.contains('.')) {
      sql = sql.replaceAll("from ", "from " + DbName + ".")
    }
    log.info(s"#####################sql = $sql")

    var data: RDD[String] = null
    if (!path.equalsIgnoreCase("") && path != null) {
      //data = sc.textFile(path)
      log.info(s"path is not supported now...")
      System.exit(1)
    } else if (!sql.equalsIgnoreCase("") && sql != null) {
      data = sqlContext.sql(sql).map(r => {
        //val s = r.mkString(",").split(",")
        //s.filter(!_.equalsIgnoreCase("0")).mkString(" ")
        val a = ArrayBuffer[String]()
        a += r.getString(0)
        for (i <- 1 until r.size) {
          if (!r.getString(i).equals("0")) a += i.toString
        }
        a.mkString(" ")
      })
    } else {
      log.info(s"Nothing can be used as training data, both path and sql are not set")
      System.exit(1)
    }
    //val data = sc.textFile(path) //"data/mllib/sample_fpgrowth.txt"

    val transactions: RDD[(String, Array[String])] = data.map(s => {
      val a = s.trim.split(' ')
      (a(0), a.tail)
    })

    //var res = List[TrainResult]()
    var res1 = List[FPGItems]()
    var res2 = List[FPGRules]()
    val minSupports = minSupport.split(",")

    for (minsup <- minSupports) {
      val timestamp = System.currentTimeMillis().toString
      val fpg = new FPGrowth().setMinSupport(minsup.toDouble).setNumPartitions(numPartitions)
      val model = fpg.run(transactions.map(_._2))
      //Items:计算输出出现频率最高的项集；Rules: 计算输出可信度较
      if (outmode.equalsIgnoreCase("Items")) {
        model.freqItemsets.collect().foreach { itemset =>
          val str = itemset.items.mkString("[", ",", "]") + ", " + itemset.freq
          log.info(str)
          val oneTrain = FPGItems(jobID, itemset.items.mkString("[", ",", "]"), itemset.freq, timestamp)
          res1 = res1.::(oneTrain)
        }
      } else if (outmode.equalsIgnoreCase("Rules")) {
        if (minConfidence != 0.0) {
          model.generateAssociationRules(minConfidence).collect().foreach { rule =>
            val str = rule.antecedent.mkString("[", ",", "]") + " => " +
              rule.consequent.mkString("[", ",", "]") + ", " +
              rule.confidence
            log.info(str)
            val oneTrain = FPGRules(jobID, rule.antecedent.mkString("[", ",", "]"),
              rule.consequent.mkString("[", ",", "]"), rule.confidence, timestamp)
            res2 = res2.::(oneTrain)
          }
        }
      } else {
        log.info("the value of output is not one of (\"Items\",\"Rulse\")")
      }
    }

    val newName = if (hiveTable.contains(".") || DbName.trim.equalsIgnoreCase("")) hiveTable
    else DbName + "." + hiveTable
    if (outmode.equalsIgnoreCase("Items")) {
      val msgDf = sc.parallelize(res1).toDF()
      msgDf.write.format("orc").mode("append").saveAsTable(newName+"_items")
      log.info(s"FPGrowth items results have been save into table ${newName}_items")
    }else if (outmode.equalsIgnoreCase("Rules")){
      val msgDf = sc.parallelize(res2).toDF()
      msgDf.write.format("orc").mode("append").saveAsTable(newName+"_rules")
      log.info(s"FPGrowth rules results have been save into table ${newName}_rules")
    }
    sc.stop()
  }
}
