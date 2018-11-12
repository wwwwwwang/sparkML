package com.datageek.mllib.train.clustering

import java.util.Properties

import com.datageek.util._
import org.apache.commons.logging.LogFactory
import org.apache.spark.mllib.clustering.BisectingKMeans
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Random

object BisectingKMeans {
  val log = LogFactory.getLog(BisectingKMeans.getClass)

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("BisectingKMeans")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)
    val sqlContext = new HiveContext(sc)
    import sqlContext.implicits._

    val arg = args(0)
    val Array(configName, jobID) = arg.split("@2@")

    var path = ""
    var sql = ""
    var k = "4"
    var maxIterations = 20
    var minDivisibleClusterSize = 1.0
    var savePath = ""
    var hiveTable = ""
    var hasLabel = false

    var tmpjobid = jobID
    if(jobID.contains(","))
      tmpjobid = jobID.split(",")(0)

    val myjdbc = new MysqlJDBCDao()
    val configs = myjdbc.getConfig(tmpjobid)
    if (configs.containsKey("rdd")) {
      sql = configs.get("rdd")
    }
    if (configs.containsKey("path")) {
      path = configs.get("path")
    }
    if (configs.containsKey("haslabel")) {
      hasLabel = configs.get("haslabel").toBoolean
    }
    if (configs.containsKey("maxiterations")) {
      maxIterations = configs.get("maxiterations").toInt
    }
    if (configs.containsKey("k")) {
      k = configs.get("k")
    }
    if (configs.containsKey("mindivisibleclustersize")) {
      minDivisibleClusterSize = configs.get("mindivisibleclustersize").toDouble
    }
    if (configs.containsKey("saveresult")) {
      hiveTable = configs.get("saveresult")
    }
    if (configs.containsKey("savemodel")) {
      savePath = configs.get("savemodel")
    }

    log.info(s"#####################path = $path")
    log.info(s"#####################iterations = $maxIterations")
    log.info(s"#####################k = $k")
    log.info(s"#####################minDivisibleClusterSize = $minDivisibleClusterSize")
    log.info(s"#####################savePath = $savePath")
    log.info(s"#####################hiveTable = $hiveTable")
    log.info(s"#####################hasLabel = $hasLabel")

    val connectionProperties = new Properties
    connectionProperties.put("user", myjdbc.dbUser)
    connectionProperties.put("password", myjdbc.dbPassword)
    val url = myjdbc.dbUrl
    val DbName = myjdbc.getDbNameInDictionary
    myjdbc.updateMLTrainAppId(tmpjobid,sc.applicationId)
    myjdbc.closeConn()
    val timestamp = System.currentTimeMillis().toString
    if(!sql.contains('.')){
      sql = sql.replaceAll("from ","from "+DbName+".")
    }
    log.info(s"#####################sql = $sql")

    /*var appid = List[ApplicationId]()
    val oneid = ApplicationId(jobID, sc.appName, sc.applicationId, timestamp)
    appid = appid.::(oneid)
    val idDF = sc.parallelize(appid).toDF()
    idDF.write.mode("append").jdbc(url, "applicationID", connectionProperties)*/

    var data: RDD[String] = null
    if (!path.equalsIgnoreCase("") && path != null) {
      //data = sc.textFile(path)
      log.info(s"path is not supported now...")
      System.exit(1)
    } else if (!sql.equalsIgnoreCase("") && sql != null) {
      val hContext = new HiveContext(sc)
      data = hContext.sql(sql).rdd.map(_.mkString(" "))
      //hContext.sql(sql).map(r => {val a = r.toString.split(",")
      //LabeledPoint(a.last.toDouble, Vectors.dense(a.take(a.length-1).map(_.toDouble)))})
    } else {
      log.info(s"Nothing can be used as training data, both path and sql are not set")
      System.exit(1)
    }

    val parsedData = data.map(s =>{
      val cols = s.trim.split(' ')
      if(hasLabel){
        (cols(0),cols(1).toDouble,Vectors.dense(cols.drop(2).map(_.toDouble)))
      }else{
        (cols(0),-1.0,Vectors.dense(cols.drop(1).map(_.toDouble)))
      }
    }).cache()

    var res = List[TrainResult]()
    var best = 0
    var cnt = 0
    var f1 = -0.1
    var mse = 1E10

    val ks = k.split(",")
    for(k <- ks) {
      val bkm = new BisectingKMeans().setK(k.toInt)
        .setMaxIterations(maxIterations)
        .setMinDivisibleClusterSize(minDivisibleClusterSize)
        .setSeed(Random.nextLong())
      val model = bkm.run(parsedData.map(_._3))

      val predictionAndLabel = parsedData.map(point =>{
        val score = model.predict(point._3)
        (point._1, point._2, score.toDouble, jobID + "_" + cnt)
      }).cache()
      if (hasLabel) {
        val newLabels = Relabel.run(predictionAndLabel.map(a=>(a._2,a._3)), k.toInt)
        val metrics = new MulticlassMetrics(newLabels)
        val F_measure = metrics.fMeasure
        log.info(s"#####################F_measure = $F_measure")
        if (F_measure > f1) {
          best = cnt
          f1 = F_measure
        }
        val alg = s"number: $cnt, k: $k, maxIterations: $maxIterations, " +
          s"minDivisibleClusterSize: $minDivisibleClusterSize, F_measure: $F_measure"
        val oneTrain = TrainResult(jobID, alg, timestamp)
        res = res.::(oneTrain)
      }else{
        val WSSSE = model.computeCost(parsedData.map(_._3))
        log.info(s"#####################Within Set Sum of Squared Errors = $WSSSE")
        if (WSSSE  < mse ) {
          best = cnt
          mse = WSSSE
        }
        val alg = s"number: $cnt, k: $k, maxIterations: $maxIterations, " +
          s"minDivisibleClusterSize: $minDivisibleClusterSize, MSE: $WSSSE"
        val oneTrain = TrainResult(jobID, alg, timestamp)
        res = res.::(oneTrain)
      }

      if (!hiveTable.equalsIgnoreCase("") && hiveTable != null) {
        val newName = if (hiveTable.contains(".") || DbName.trim.equalsIgnoreCase("")) hiveTable
        else DbName + "." + hiveTable
        if(hasLabel){
          val df = Relabel.run1(predictionAndLabel, k.toInt).map(a => PredictAndLabel(a._1, a._2, a._3, a._4, timestamp)).toDF()
          df.write.mode("append").format("orc").saveAsTable(newName)
        }else{
          val df = predictionAndLabel.map(a => PredictAndLabel(a._1, a._2, a._3, a._4, timestamp)).toDF()
          df.write.mode("append").format("orc").saveAsTable(newName)
        }
        log.info(s"the prediction label is saved in " + newName)
      } else {
        log.info(s"hiveTable is empty, the prediction label don't need to be saved....")
      }

      predictionAndLabel.unpersist()

      if (!savePath.equalsIgnoreCase("") && savePath != null) {
        //model.save(sc, savePath)
        log.info(s"save is not supported in bisecting k-means...")
      } else {
        log.info(s"savePath is empty, no needing to save....")
      }
      cnt += 1
    }

    if (cnt != 1 && hasLabel) {
      val msg = "In K_means job " + jobID + ", the best result is got by " +
        best + "-th group parameters, and the max F_measure is " + f1
      val onemsg = TrainResult(jobID, msg, timestamp)
      res = res.::(onemsg)
    }
    val msgDf = sc.parallelize(res).toDF()
    msgDf.write.mode("append").jdbc(url, "train_information", connectionProperties)

    sc.stop()
  }
}
