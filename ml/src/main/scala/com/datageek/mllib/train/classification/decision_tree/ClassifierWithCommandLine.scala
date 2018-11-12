package com.datageek.mllib.train.classification.decision_tree

import org.apache.commons.cli._
import org.apache.commons.logging.LogFactory
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.{SparkConf, SparkContext}

object ClassifierWithCommandLine {
  val log = LogFactory.getLog(Classifier.getClass)

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("decision_tree classifier")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)

    val opt = new Options()
    opt.addOption("p", "path", true, "the path of training data  *")
    opt.addOption("r", "ratio", true, "the ratio of training data used for training ")
    opt.addOption("n", "num-classes", true, "number of classes")
    opt.addOption("i", "impurity", true, "impurity measure: gini or entropy")
    opt.addOption("d", "max-depth", true, "the max depth of a tree")
    opt.addOption("b", "max-bins", true, "the max bins of a tree")
    opt.addOption("s", "save-path", true, "the model will be saved as this path *")
    opt.addOption("h", "help", false, "help message")

    var path = ""
    var ratio = 1.0
    var numClasses = 2
    var impurity = "gini"
    var maxDepth = 5
    var maxBins = 32
    var savePath = ""

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
    if (cl.hasOption("r")) {
      ratio = cl.getOptionValue("r").toDouble
    }
    if (cl.hasOption("n")) {
      numClasses = cl.getOptionValue("n").toInt
    }
    if (cl.hasOption("i")) {
      impurity = cl.getOptionValue("i")
    }
    if (cl.hasOption("d")) {
      maxDepth = cl.getOptionValue("d").toInt
    }
    if (cl.hasOption("b")) {
      maxBins = cl.getOptionValue("b").toInt
    }
    if (cl.hasOption("s")) {
      savePath = cl.getOptionValue("s")
    }
    log.info("#####################path =" + path)
    log.info("#####################ratio =" + ratio)
    log.info("#####################numClasses =" + numClasses)
    log.info("#####################impurity =" + impurity)
    log.info("#####################maxDepth =" + maxDepth)
    log.info("#####################maxBins =" + maxBins)
    log.info("#####################savePath =" + savePath)

    val data = MLUtils.loadLibSVMFile(sc, path)
    var trainingData = data
    var testData = data
    if (ratio < 1.0) {
      val splits = data.randomSplit(Array(ratio, 1 - ratio))
      trainingData = splits(0)
      testData = splits(1)
    }

    val categoricalFeaturesInfo = Map[Int, Int]()
    val model = DecisionTree.trainClassifier(trainingData, numClasses, categoricalFeaturesInfo,
      impurity, maxDepth, maxBins)
    // Evaluate model on test instances and compute test error
    if (ratio < 1.0) {
      val labelAndPreds = testData.map { point =>
        val prediction = model.predict(point.features)
        (point.label, prediction)
      }
      val testErr = labelAndPreds.filter(r => r._1 != r._2).count().toDouble / testData.count()
      log.info("#####################Test Error = " + testErr)
      log.info("#####################Learned classification tree model:\n" + model.toDebugString)
    }

    model.save(sc, savePath)
    //val sameModel = DecisionTreeModel.load(sc, "target/tmp/myDecisionTreeClassificationModel")
    sc.stop()
  }
}
