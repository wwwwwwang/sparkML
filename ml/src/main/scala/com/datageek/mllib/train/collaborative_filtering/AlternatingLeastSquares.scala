package com.datageek.mllib.train.collaborative_filtering

import java.util.Properties

import com.datageek.util._
import org.apache.commons.logging.LogFactory
import org.apache.spark.mllib.evaluation.RegressionMetrics
import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

object AlternatingLeastSquares{
  val log = LogFactory.getLog(AlternatingLeastSquares.getClass)

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("AlternatingLeastSquares")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)
    val sqlContext = new HiveContext(sc)
    import sqlContext.implicits._

    val arg = args(0)
    val Array(configName, jobID) = arg.split("@2@")

    var path = ""
    var sql = ""
    var rank = "10"
    var iterations = 20
    var lambda = "0.01"
    var alpha = "0.01"
    var nonnegative = false
    var savePath = ""
    var hiveTable = ""
    var hasTest = true

    var tmpjobid = jobID
    if(jobID.contains(","))
      tmpjobid = jobID.split(",")(0)

    val myjdbc = new MysqlJDBCDao()
    val configs = myjdbc.getConfig(tmpjobid)
    if (configs.containsKey("path")) {
      path = configs.get("path")
    }
    if (configs.containsKey("sql")) {
      sql = configs.get("sql")
    }
    if (configs.containsKey("iterations")) {
      iterations = configs.get("iterations").toInt
    }
    if (configs.containsKey("rank")) {
      rank = configs.get("rank")
    }
    if (configs.containsKey("lambda")) {
      lambda = configs.get("lambda")
    }
    if (configs.containsKey("alpha")) {
      alpha = configs.get("alpha")
    }
    if (configs.containsKey("nonnegative")) {
      nonnegative = configs.get("nonnegative").toBoolean
    }
    if (configs.containsKey("saveresult")) {
      hiveTable = configs.get("saveresult")
    }
    if (configs.containsKey("savepath")) {
      savePath = configs.get("savepath")
    }
    if (configs.containsKey("hastest")) {
      hasTest = configs.get("hastest").toBoolean
    }

    log.info(s"#####################path = $path")
    log.info(s"#####################rank = $rank")
    log.info(s"#####################iterations = $iterations")
    log.info(s"#####################lambda = $lambda")
    log.info(s"#####################nonnegative = $nonnegative")
    log.info(s"#####################alpha = $alpha")
    log.info(s"#####################savePath = $savePath")
    log.info(s"#####################hasTest = $hasTest")

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
      data = hContext.sql(sql).rdd.map(_.mkString(","))
      //hContext.sql(sql).map(r => {val a = r.toString.split(",")
      //LabeledPoint(a.last.toDouble, Vectors.dense(a.take(a.length-1).map(_.toDouble)))})
    } else {
      log.info(s"Nothing can be used as training data, both path and sql are not set")
      System.exit(1)
    }
    val ratings = data.map(_.split(',') match { case Array(user, item, rate) =>
      Rating(user.toInt, item.toInt, rate.toDouble)
    })
    val usersProducts = ratings.map {
      case Rating(user, product, rate) => (user, product)
    }
    if (hasTest) {
      ratings.cache()
      usersProducts.cache()
    }

    var res = List[TrainResult]()
    var best = 0
    var cnt = 0
    var mse = 1E10

    val ranks = rank.split(",")
    val lambdas = lambda.split(",")
    val alphas = alpha.split(",")

    for(rank <- ranks; lambda <- lambdas; alpha <- alphas){
      val alsAlg = new ALS().setNonnegative(nonnegative).setAlpha(alpha.toDouble)
        .setIterations(iterations).setLambda(lambda.toDouble).setRank(rank.toInt)
      val model = alsAlg.run(ratings)

      if (hasTest) {
        val predictions = model.predict(usersProducts).map {
          case Rating(user, product, rate) => ((user, product), rate)
        }
        val ratesAndPreds = ratings.map {
          case Rating(user, product, rate) => ((user, product), rate)
        }.join(predictions)
        ratesAndPreds.cache()
        /*val MSE = ratesAndPreds.map {
          case ((user, product), (r1, r2)) => Math.pow(r1 - r2, 2)
        }.mean()
        log.info(s"#####################Mean Squared Error = $MSE")*/
        val regressionMetrics = new RegressionMetrics(ratesAndPreds.map(_._2))
        val RMSE = regressionMetrics.rootMeanSquaredError
        log.info(s"#####################RMSE = $RMSE")
        if (RMSE < mse) {
          best = cnt
          mse = RMSE
        }
        val alg = s"number: $cnt, rank: $rank, iterations: $iterations, lambda: $lambda, " +
          s"nonnegative: $nonnegative, alpha: $alpha, rmse: $RMSE"
        val oneTrain = TrainResult(jobID, alg, timestamp)
        res = res.::(oneTrain)
        if (!hiveTable.equalsIgnoreCase("") && hiveTable != null) {
          val newName = if (hiveTable.contains(".") || DbName.trim.equalsIgnoreCase("")) hiveTable
          else DbName + "." + hiveTable
          val df = ratesAndPreds.map(a => PredictAndLabel(a._1._1.toString, a._1._2, a._2._1, a._2._2.toString, timestamp)).toDF()
          df.write.mode("append").format("orc").saveAsTable(newName)
          log.info(s"the prediction label is saved in " + newName)
        } else {
          log.info(s"hiveTable is empty, the prediction label don't need to be saved....")
        }
        ratesAndPreds.unpersist()
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
        log.info(s"savePath is empty, no needing to save....")
      }
      cnt += 1
    }

    if (cnt != 1) {
      val msg = "In AlternatingLeastSquares job " + jobID + ", the best result is got by " +
        best + "-th group parameters, and the min RMSE is " + mse
      val onemsg = TrainResult(jobID, msg, timestamp)
      res = res.::(onemsg)
    }
    val msgDf = sc.parallelize(res).toDF()
    msgDf.write.mode("append").jdbc(url, "train_information", connectionProperties)

    //val sameModel = MatrixFactorizationModel.load(sc, "target/tmp/myCollaborativeFilter")
    sc.stop()
  }
}
