package com.datageek.mllib.predict

import java.util.Properties

import com.datageek.util.{ApplicationId, MysqlJDBCDao, PredictOnly}
import org.apache.commons.logging.LogFactory
import org.apache.spark.mllib.classification.{LogisticRegressionModel, NaiveBayesModel, SVMModel}
import org.apache.spark.mllib.clustering.{GaussianMixtureModel, KMeansModel}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import org.apache.spark.mllib.regression.{IsotonicRegressionModel, LinearRegressionModel}
import org.apache.spark.mllib.tree.model.{DecisionTreeModel, GradientBoostedTreesModel, RandomForestModel}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by SNOW on 2017/3/11.
  */
object Prediction {
  val log = LogFactory.getLog(Prediction.getClass)

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Prediction")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)
    val sqlContext = new HiveContext(sc)
    import sqlContext.implicits._

    val jobID = args(0)

    var sql = ""
    var muuid = ""
    var dbtype = "hive"
    var hiveTable = ""
    var alId = 0
    var modelPath = ""

    var tmpjobid = jobID
    if(jobID.contains(","))
      tmpjobid = jobID.split(",")(0)

    log.info(s"#####################jobID = $jobID")
    log.info(s"#####################tmpjobid = $tmpjobid")

    val myjdbc = new MysqlJDBCDao()
    val configs = myjdbc.getPredictConfig(tmpjobid)
    if (configs.containsKey("sql")) {
      sql = configs.get("sql")
    }
    if (configs.containsKey("muuid")) {
      muuid = configs.get("muuid")
    }
    if (configs.containsKey("dbtype")) {
      dbtype = configs.get("dbtype")
    }
    if (configs.containsKey("tablename")) {
      hiveTable = configs.get("tablename")
    }
    alId = myjdbc.getAlIdInDictionary(muuid).toInt
    modelPath = myjdbc.getModelPathInDictionary(muuid)

    log.info(s"#####################sql = $sql")
    log.info(s"#####################muuid = $muuid")
    log.info(s"#####################dbtype = $dbtype")
    log.info(s"#####################hiveTable = $hiveTable")
    log.info(s"#####################alId = $alId")
    log.info(s"#####################modelPath = $modelPath")

    val connectionProperties = new Properties
    connectionProperties.put("user", myjdbc.dbUser)
    connectionProperties.put("password", myjdbc.dbPassword)
    val url = myjdbc.dbUrl
    val DbName = myjdbc.getDbNameInDictionary
    myjdbc.updateMLPredictAppId(tmpjobid, sc.applicationId)
    myjdbc.closeConn()
    val timestamp = System.currentTimeMillis().toString

    /*if (!sql.contains('.')) {
      sql = sql.replaceAll("from ", "from " + DbName + ".")
    }
    log.info(s"#####################sql = $sql")*/

    /*var appid = List[ApplicationId]()
    val oneid = ApplicationId(jobID, sc.appName, sc.applicationId, timestamp)
    appid = appid.::(oneid)
    val idDF = sc.parallelize(appid).toDF()
    idDF.write.mode("append").jdbc(url, "applicationID", connectionProperties)*/


    process(sc, sqlContext, jobID, sql, alId, modelPath, dbtype, hiveTable, DbName)
  }

  def process(sc: SparkContext, hContext: HiveContext, jobID: String, sql: String, alId: Int,
              modelPath: String, dbtype:String, hiveTable: String, DbName: String): Unit = {
    val timestamp = System.currentTimeMillis().toString
    import hContext.implicits._
    hContext.sql("use " + DbName)
    log.info("+++++++++++++++++++++++++ use " + DbName)
    val data = hContext.sql(sql).rdd.map(s => {
      val columns = s.mkString(" ").trim.split(" ")
      (columns(0), Vectors.dense(columns.drop(1).map(_.toDouble)))
    }).cache()

    var prediction: RDD[(String, Double)] = null
    alId match {
      case 1 => val model = SVMModel.load(sc, modelPath)
        prediction = data.map(s => {
          val res = model.predict(s._2)
          (s._1, res.toDouble)
        })
      case 2 => val model = LogisticRegressionModel.load(sc, modelPath)
        prediction = data.map(s => {
          val res = model.predict(s._2)
          (s._1, res.toDouble)
        })
      case 3 => val model = NaiveBayesModel.load(sc, modelPath)
        prediction = data.map(s => {
          val res = model.predict(s._2)
          (s._1, res.toDouble)
        })
      case 4 | 8 => val model = DecisionTreeModel.load(sc, modelPath)
        prediction = data.map(s => {
          val res = model.predict(s._2)
          (s._1, res.toDouble)
        })
      case 5 | 9 => val model = RandomForestModel.load(sc, modelPath)
        prediction = data.map(s => {
          val res = model.predict(s._2)
          (s._1, res.toDouble)
        })
      case 6 | 10 => val model = GradientBoostedTreesModel.load(sc, modelPath)
        prediction = data.map(s => {
          val res = model.predict(s._2)
          (s._1, res.toDouble)
        })
      case 7 => val model = LinearRegressionModel.load(sc, modelPath)
        prediction = data.map(s => {
          val res = model.predict(s._2)
          (s._1, res.toDouble)
        })
      case 11 => val model = IsotonicRegressionModel.load(sc, modelPath)
        prediction = data.map(s => {
          val res = model.predict(s._2(0)) //only predict the first column
          (s._1, res.toDouble)
        })
      case 12 => //val model = MatrixFactorizationModel.load(sc, modelPath)
        /*prediction = data.map(s =>{
          val res = model.predict(s._2)
          (s._1, res.toDouble)
        })*/
        log.info("waiting to add ...")
      case 13 => val model = KMeansModel.load(sc, modelPath)
        prediction = data.map(s => {
          val res = model.predict(s._2)
          (s._1, res.toDouble)
        })
      case 14 => val model = GaussianMixtureModel.load(sc, modelPath)
        prediction = data.map(s => {
          val res = model.predict(s._2)
          (s._1, res.toDouble)
        })
      case 15 | _ =>
        log.info("Not support to load model and predict")
    }

    if(dbtype.equalsIgnoreCase("hive")){
      if (!hiveTable.equalsIgnoreCase("") && hiveTable != null) {
        val newName = if (hiveTable.contains(".") || DbName.trim.equalsIgnoreCase("")) hiveTable
        else DbName + "." + hiveTable
        val df = prediction.map(a => PredictOnly(a._1, a._2, jobID, timestamp)).toDF()
        df.write.mode("append").format("orc").saveAsTable(newName)
        log.info(s"the prediction label is saved in " + newName)
      } else {
        log.info(s"hiveTable is empty, the prediction label don't need to be saved....")
      }
    }else{
      log.info("database " + dbtype + " is not supported now, will be added in the future!")
    }

  }

}
