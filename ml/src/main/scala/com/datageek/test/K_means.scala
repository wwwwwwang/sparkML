package com.datageek.test

import com.datageek.util.Relabel
import org.apache.commons.cli._
import org.apache.commons.logging.LogFactory
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.feature.PCA
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

import scala.util.Random

object K_means {
  val log = LogFactory.getLog(K_means.getClass)

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("K_means test")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)

    val opt = new Options()
    opt.addOption("p", "path", true, "the path of training data  *")
    //opt.addOption("r", "ratio", true, "the ratio of training data used for training ")
    opt.addOption("n", "num-classes", true, "number of classes")
    opt.addOption("i", "num-iterations", true, "number of iterations in one time")
    opt.addOption("r", "runs", true, "the runs times for getting model")
    opt.addOption("q", "sql", true, "the training data will be produced by this sql")
    opt.addOption("s", "save-path", true, "the model will be saved as this path *")
    opt.addOption("z", "zero-based", false, "whether the label is zero based")
    opt.addOption("d", "dimension-reduction", false, "whether using pca to reduce dimension")
    opt.addOption("h", "help", false, "help message")

    var path = ""
    var numClusters = 3
    var iterations = 50
    var runs = 10
    var savePath = ""
    var sql = ""
    var zeroBased = true
    var dimensionReduction = false

    val formatstr = "sh run.sh yarn-cluster|yarn-client|local .....(with * is essential)"
    val formatter: HelpFormatter = new HelpFormatter()
    val parser: CommandLineParser = new PosixParser()
    var cl: CommandLine = null
    try {
      cl = parser.parse(opt, args)
    } catch {
      case e: org.apache.commons.cli.ParseException =>
        formatter.printHelp(formatstr, opt)
        System.exit(1)
    }
    if (cl.hasOption("h")) {
      formatter.printHelp(formatstr, opt)
      System.exit(0)
    }
    if (cl.hasOption("p")) {
      path = cl.getOptionValue("p")
    }
    if (cl.hasOption("i")) {
      iterations = cl.getOptionValue("i").toInt
    }
    if (cl.hasOption("n")) {
      numClusters = cl.getOptionValue("n").toInt
    }
    if (cl.hasOption("r")) {
      runs = cl.getOptionValue("r").toInt
    }
    if (cl.hasOption("s")) {
      savePath = cl.getOptionValue("s")
    }
    if (cl.hasOption("q")) {
      sql = cl.getOptionValue("q")
    }
    if (cl.hasOption("z")) {
      zeroBased = false
    }
    if (cl.hasOption("d")) {
      dimensionReduction = true
    }

    log.info(s"#####################path = $path")
    log.info(s"#####################iterations = $iterations")
    log.info(s"#####################numClusters = $numClusters")
    log.info(s"#####################runs = $runs")
    log.info(s"#####################sql = $sql")
    log.info(s"#####################savePath = $savePath")
    log.info(s"#####################zeroBased = $zeroBased")
    log.info(s"#####################dimensionReduction = $dimensionReduction")

    //val data = MLUtils.loadLibSVMFile(sc, path)
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

    val parsedData = data.map { line =>
      /*val parts = line.split(" ")
      LabeledPoint(if(zeroBased) {parts(0).toDouble} else parts(0).toDouble -1,
        Vectors.dense(parts(1).split(' ').map(_.toDouble)))*/
      /*val a = line.split(" ")
      LabeledPoint(a.last.toDouble, Vectors.dense(a.take(a.length-1).map(_.toDouble)))*/
      val a = line.split(" ")
      LabeledPoint(a.head.toDouble, Vectors.dense(a.tail.map(_.toDouble)))
    }.cache()

    //val parsedData = data.map(s => Vectors.dense(s.split(' ').map(_.toDouble))).cache()
    var trainingData = parsedData.map(s => s.features)
    if (dimensionReduction) {
      val pca = new PCA(2).fit(parsedData.map(_.features))
      val pcaData = parsedData.map(p => p.copy(features = pca.transform(p.features)))
      trainingData = pcaData.map(s => s.features)
    }

    val clusters = KMeans.train(trainingData, numClusters, iterations, runs,
      KMeans.K_MEANS_PARALLEL, Random.nextLong())

    trainingData.foreach(v => {
      log.info(s"#####################whsh--->${v.toString} belong to cluster ${clusters.predict(v)}")
    })
    var predictionAndLabel: RDD[(Double, Double)] = null
    if (dimensionReduction) {
      val pca = new PCA(2).fit(parsedData.map(_.features))
      val pcaData = parsedData.map(p => p.copy(features = pca.transform(p.features)))
      predictionAndLabel = pcaData.map(p => (p.label, clusters.predict(p.features).toDouble))
    } else {
      predictionAndLabel = parsedData.map(p => (p.label, clusters.predict(p.features).toDouble))
    }
    predictionAndLabel.foreach(v => {
      log.info(s"old label pair: ${v._1} --- ${v._2}")
    })
    val newLabels = Relabel.run(predictionAndLabel, numClusters)
    newLabels.foreach(v => {
      log.info(s"new label pair: ${v._1} --- ${v._2}")
    })

    val metrics = new MulticlassMetrics(newLabels)
    log.info(s"#####################F_measure = ${metrics.fMeasure}")
    val WSSSE = clusters.computeCost(trainingData)
    log.info(s"#####################Within Set Sum of Squared Errors = $WSSSE")

    if (savePath != null && !savePath.trim.equalsIgnoreCase("")) {
      clusters.save(sc, savePath)
    }
    //val sameModel = KMeansModel.load(sc, "target/org/apache/spark/KMeansExample/KMeansModel")
    log.info(s"==(-_-)~====END====(-_-)~==")
    sc.stop()
  }
}
