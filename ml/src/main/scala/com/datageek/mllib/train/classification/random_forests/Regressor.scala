package com.datageek.mllib.train.classification.random_forests

import java.util.Properties

import com.datageek.util._
import org.apache.commons.logging.LogFactory
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

object Regressor {
  val log = LogFactory.getLog(Regressor.getClass)

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("random_forest regressor")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)
    val sqlContext = new HiveContext(sc)
    import sqlContext.implicits._

    val arg = args(0)
    val Array(configName, jobID) = arg.split("@2@")

    var path = ""
    var sql = ""
    var ratio = 1.0
    var numTrees = "3"
    var impurity = "variance"
    var maxDepth = "5"
    var maxBins = 32
    var savePath = ""
    var hiveTable = ""
    var categoricalFeatures = ""
    var featureSubsetStrategy = "auto"

    var tmpjobid = jobID
    if(jobID.contains(","))
      tmpjobid = jobID.split(",")(0)

    val myjdbc = new MysqlJDBCDao()
    val configs = myjdbc.getConfig(tmpjobid)
    if (configs.containsKey("data")) {
      sql = configs.get("data")
    }
    if (configs.containsKey("path")) {
      path = configs.get("path")
    }
    if (configs.containsKey("ratio")) {
      ratio = configs.get("ratio").toDouble
    }
    if (configs.containsKey("numtrees")) {
      numTrees = configs.get("numtrees")
    }
    if (configs.containsKey("impurity")) {
      impurity = configs.get("impurity")
    }
    if (configs.containsKey("maxdepth")) {
      maxDepth = configs.get("maxdepth")
    }
    if (configs.containsKey("maxbins")) {
      maxBins = configs.get("maxbins").toInt
    }
    if (configs.containsKey("featuresubsetstrategy")) {
      featureSubsetStrategy = configs.get("featuresubsetstrategy")
    }
    if (configs.containsKey("saveresult")) {
      hiveTable = configs.get("saveresult")
    }
    if (configs.containsKey("savemodel")) {
      savePath = configs.get("savemodel")
    }
    if (configs.containsKey("categoricalfeaturesinfo")) {
      categoricalFeatures = configs.get("categoricalfeaturesinfo")
    }

    log.info("#####################path =" + path)
    log.info("#####################ratio =" + ratio)
    log.info(s"#####################numClasses = $numTrees")
    log.info("#####################impurity =" + impurity)
    log.info("#####################maxDepth =" + maxDepth)
    log.info("#####################maxBins =" + maxBins)
    log.info("#####################savePath =" + savePath)
    log.info("#####################hiveTable =" + hiveTable)
    log.info(s"#####################featureSubsetStrategy =  $featureSubsetStrategy")

    val categoricalFeaturesInfo = MakeCategoricalFeatures.make(categoricalFeatures)
    //val categoricalFeaturesInfo = Map[Int, Int]()
    log.info("#####################categoricalFeaturesInfo =" + categoricalFeaturesInfo.toString())

    val connectionProperties = new Properties
    connectionProperties.put("user", myjdbc.dbUser)
    connectionProperties.put("password", myjdbc.dbPassword)
    val url = myjdbc.dbUrl
    val DbName = myjdbc.getDbNameInDictionary
    myjdbc.updateMLTrainAppId(tmpjobid,sc.applicationId)
    myjdbc.closeConn()

    if(!sql.contains('.')){
      sql = sql.replaceAll("from ","from "+DbName+".")
    }
    log.info(s"#####################sql = $sql")

    val timestamp = System.currentTimeMillis().toString

    /*var appid = List[ApplicationId]()
    val oneid = ApplicationId(jobID, sc.appName, sc.applicationId, timestamp)
    appid = appid.::(oneid)
    val idDF = sc.parallelize(appid).toDF()
    idDF.write.mode("append").jdbc(url, "applicationID", connectionProperties)*/

    var data: RDD[(String, LabeledPoint)] = null
    if (!path.equalsIgnoreCase("") && path != null) {
      //data = MLUtils.loadLibSVMFile(sc, path)
      log.info(s"path is not supported now...")
      System.exit(1)
    } else if (!sql.equalsIgnoreCase("") && sql != null) {
      data = sqlContext.sql(sql).map(r => {
        val a = r.mkString(",").split(",")
        (a(0), LabeledPoint(a(1).toDouble, Vectors.dense(a.drop(2).map(_.toDouble))))
        //LabeledPoint(a.last.toDouble, Vectors.dense(a.take(a.length - 1).map(_.toDouble)))
        //LabeledPoint(a.head.toDouble, Vectors.dense(a.tail.map(_.toDouble)))
      })
    } else {
      log.info(s"Nothing can be used as training data, both path and sql are not set")
      System.exit(1)
    }
    data.cache()

    var trainingData = data
    var testData = data
    if (ratio < 1.0) {
      val splits = data.randomSplit(Array(ratio, 1 - ratio))
      trainingData = splits(0)
      testData = splits(1)
      testData.cache()
    }
    trainingData.cache()
    data.unpersist()

    var res = List[TrainResult]()
    var best = 0
    var cnt = 0
    var mse = 1E10

    val depths = maxDepth.split(",")
    val trees = numTrees.split(",")

    for (depth <- depths; tree <- trees) {
      val model = RandomForest.trainRegressor(trainingData.map(_._2), categoricalFeaturesInfo,
        tree.toInt, featureSubsetStrategy, impurity, depth.toInt, maxBins)

      if (ratio < 1.0) {
        val scoreAndLabels = testData.map { point =>
          val score = model.predict(point._2.features)
          (point._1, point._2.label, score, jobID + "_" + cnt)
        }
        scoreAndLabels.cache()
        val MSE = scoreAndLabels.map { case (o, v, p, f) => math.pow(v - p, 2) }.mean()
        log.info(s"#####################training Mean Squared Error = $MSE")
        log.info("#####################Learned classification RF model:\n" + model.toDebugString)
        if (MSE < mse) {
          best = cnt
          mse = MSE
        }
        val alg = s"number: $cnt, categoricalFeaturesInfo: ${categoricalFeaturesInfo.toString()}," +
          s"numTrees: $tree, featureSubsetStrategy: $featureSubsetStrategy, impurity: $impurity," +
          s"maxDepth: $depth, maxBins: $maxBins, mse: $MSE"
        val oneTrain = TrainResult(jobID, alg, timestamp)
        res = res.::(oneTrain)
        if (!hiveTable.equalsIgnoreCase("") && hiveTable != null) {
          val newName = if (hiveTable.contains(".") || DbName.trim.equalsIgnoreCase("")) hiveTable
          else DbName + "." + hiveTable
          val df = scoreAndLabels.map(a => PredictAndLabel(a._1.toString, a._2, a._3, a._4, timestamp)).toDF()
          df.write.mode("append").format("orc").saveAsTable(newName)
          log.info(s"the prediction label is saved in " + newName)
        } else {
          log.info(s"hiveTable is empty, the prediction label don't need to be saved....")
        }
        scoreAndLabels.unpersist()
      }
      if (!savePath.equalsIgnoreCase("") && savePath != null) {
        val timestamp = System.currentTimeMillis().toString
        val newSavePath = savePath + "/" + timestamp
        model.save(sc, newSavePath)
        log.info(s"model has been saved in hdfs path: "+newSavePath)
        var svPath = List[ModeSavPath]()
        val onepath = ModeSavPath(jobID, sc.appName, newSavePath, timestamp)
        svPath = svPath.::(onepath)
        val pathDF = sc.parallelize(svPath).toDF()
        pathDF.write.mode("append").jdbc(url, "mode_save_path", connectionProperties)
      } else {
        log.info(s"savePath is empty, no needing for saving....")
      }
      cnt += 1
    }
    if (cnt != 1) {
      val msg = "In random_forest regressor job " + jobID + ", the best result is got by " +
        best + "-th group parameters, and the min MSE is " + mse
      val onemsg = TrainResult(jobID, msg, timestamp)
      res = res.::(onemsg)
    }
    val msgDf = sc.parallelize(res).toDF()
    msgDf.write.mode("append").jdbc(url, "train_information", connectionProperties)

    //val sameModel = RandomForestModel.load(sc, "target/tmp/myRandomForestRegressionModel")
    sc.stop()
  }
}
