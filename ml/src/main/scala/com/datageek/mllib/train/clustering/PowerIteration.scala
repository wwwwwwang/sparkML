package com.datageek.mllib.train.clustering

import com.datageek.util.MysqlJDBCDao
import org.apache.commons.logging.LogFactory
import org.apache.spark.mllib.clustering.PowerIterationClustering
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


object PowerIteration {
  val log = LogFactory.getLog(PowerIteration.getClass)

  def generateCircle(radius: Double, n: Int): Seq[(Double, Double)] = {
    Seq.tabulate(n) { i =>
      val theta = 2.0 * math.Pi * i / n
      (radius * math.cos(theta), radius * math.sin(theta))
    }
  }

  //产生同心圆样的数据模型（见程序后面的图）：参数为同心圆的个数和点数
  //第一个同心圆上有nPoints个点，第二个有2*nPoints个点，第三个有3*nPoints个点，以此类推
  def generateCirclesRdd(
                          sc: SparkContext,
                          nCircles: Int,
                          nPoints: Int): RDD[(Long, Long, Double)] = {
    val points = (1 to nCircles).flatMap { i =>
      generateCircle(i, i * nPoints)
    }.zipWithIndex
    val rdd = sc.parallelize(points)
    val distancesRdd = rdd.cartesian(rdd).flatMap { case (((x0, y0), i0), ((x1, y1), i1)) =>
      if (i0 < i1) {
        Some((i0.toLong, i1.toLong, gaussianSimilarity((x0, y0), (x1, y1))))
      } else {
        None
      }
    }
    distancesRdd
  }

  def gaussianSimilarity(p1: (Double, Double), p2: (Double, Double)): Double = {
    val ssquares = (p1._1 - p2._1) * (p1._1 - p2._1) + (p1._2 - p2._2) * (p1._2 - p2._2)
    math.exp(-ssquares / 2.0)
  }

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Power_Iteration")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)

    val configName = args(0)

    var path = ""
    var numClusters = 3
    var iterations = 50
    var runs = 10
    var savePath = ""
    var numPoints = 5
    var modeType = "random"

    val myjdbc = new MysqlJDBCDao()
    val configs = myjdbc.getConfig(configName)
    if (configs.containsKey("path")) {
      path = configs.get("path")
    }
    if (configs.containsKey("iterations")) {
      iterations = configs.get("iterations").toInt
    }
    if (configs.containsKey("numclusters")) {
      numClusters = configs.get("numclusters").toInt
    }
    if (configs.containsKey("runs")) {
      runs = configs.get("runs").toInt
    }
    if (configs.containsKey("numpoints")) {
      numPoints = configs.get("numpoints").toInt
    }
    if (configs.containsKey("savepath")) {
      savePath = configs.get("savepath")
      savePath += "/" + System.currentTimeMillis()
    }
    if (configs.containsKey("modetype")) {
      modeType = configs.get("modetype")
    }

    log.info(s"#####################path = $path")
    log.info(s"#####################iterations = $iterations")
    log.info(s"#####################numClusters = $numClusters")
    log.info(s"#####################numPoints = $numPoints")
    log.info(s"#####################runs = $runs")
    log.info(s"#####################modeType = $modeType")
    log.info(s"#####################savePath = $savePath")
    myjdbc.closeConn()

    //val data = sc.textFile(path)
    //val parsedData = data.map(s => Vectors.dense(s.split(' ').map(_.toDouble))).cache()


    val circlesRdd = generateCirclesRdd(sc, numClusters, numPoints)
    val model = new PowerIterationClustering()
      .setK(numClusters)
      .setMaxIterations(iterations)
      .setInitializationMode(modeType)
      .run(circlesRdd)

    val clusters = model.assignments.collect().groupBy(_.cluster).mapValues(_.map(_.id))
    val assignments = clusters.toList.sortBy { case (k, v) => v.length }
    val assignmentsStr = assignments
      .map { case (k, v) =>
        s"$k -> ${v.sorted.mkString("[", ",", "]")}"
      }.mkString(", ")
    val sizesStr = assignments.map {
      _._2.length
    }.sorted.mkString("(", ",", ")")
    log.info(s"#####################Cluster assignments: $assignmentsStr\ncluster sizes: $sizesStr")

    model.save(sc, savePath)
    //val sameModel = PowerIterationClusteringModel.load(sc, "myModelPath")
    sc.stop()
  }
}
