package com.datageek.mllib.train.dimensionality_reduction

import org.apache.commons.logging.LogFactory
import org.apache.spark.mllib.feature.PCA
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.linalg.{Matrix, Vector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object PrincipalComponentAnalysis {
  val log = LogFactory.getLog(PrincipalComponentAnalysis.getClass)

  def PCArun(dataRDD: RDD[Vector], num: Int): RowMatrix = {
    val mat = new RowMatrix(dataRDD)
    val pc = mat.computePrincipalComponents(num)
    mat.multiply(pc)
  }

  def PCArunWithLabel(data: RDD[LabeledPoint], num: Int): RDD[LabeledPoint] = {
    val pca = new PCA(num).fit(data.map(_.features))
    data.map(p => p.copy(features = pca.transform(p.features)))
  }

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("PrincipalComponentAnalysis")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)

    val data = Array(
      Vectors.sparse(5, Seq((1, 1.0), (3, 7.0))),
      Vectors.dense(2.0, 0.0, 3.0, 4.0, 5.0),
      Vectors.dense(4.0, 0.0, 0.0, 6.0, 7.0))
    val dataRDD = sc.parallelize(data, 2)
    val mat: RowMatrix = new RowMatrix(dataRDD)
    // Compute the top 4 principal components.
    // Principal components are stored in a local dense matrix.
    val pc: Matrix = mat.computePrincipalComponents(4)
    // Project the rows to the linear space spanned by the top 4 principal components.
    val projected: RowMatrix = mat.multiply(pc)

    sc.stop()
  }

  def sample(sc: SparkContext): Unit = {
    val data: RDD[LabeledPoint] = sc.parallelize(Seq(
      new LabeledPoint(0, Vectors.dense(1, 0, 0, 0, 1)),
      new LabeledPoint(1, Vectors.dense(1, 1, 0, 1, 0)),
      new LabeledPoint(1, Vectors.dense(1, 1, 0, 0, 0)),
      new LabeledPoint(0, Vectors.dense(1, 0, 0, 0, 0)),
      new LabeledPoint(1, Vectors.dense(1, 1, 0, 0, 0))))
    // Compute the top 5 principal components.
    val pca = new PCA(5).fit(data.map(_.features))
    // Project vectors to the linear space spanned by the top 5 principal
    // components, keeping the label
    val projected = data.map(p => p.copy(features = pca.transform(p.features)))
  }
}
