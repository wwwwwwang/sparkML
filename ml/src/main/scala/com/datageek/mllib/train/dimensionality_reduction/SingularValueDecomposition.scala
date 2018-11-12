package com.datageek.mllib.train.dimensionality_reduction

import org.apache.commons.logging.LogFactory
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.linalg.{Matrix, SingularValueDecomposition, Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


object SingularValueDecomposition {
  val log = LogFactory.getLog(SingularValueDecomposition.getClass)

  def SVDrun(dataRDD: RDD[Vector], num: Int): SingularValueDecomposition[RowMatrix, Matrix] = {
    val mat: RowMatrix = new RowMatrix(dataRDD)
    mat.computeSVD(num, computeU = true)
  }

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("SingularValueDecomposition")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)

    val data = Array(
      Vectors.sparse(5, Seq((1, 1.0), (3, 7.0))),
      Vectors.dense(2.0, 0.0, 3.0, 4.0, 5.0),
      Vectors.dense(4.0, 0.0, 0.0, 6.0, 7.0))

    val dataRDD = sc.parallelize(data, 2)

    val mat: RowMatrix = new RowMatrix(dataRDD)

    // Compute the top 5 singular values and corresponding singular vectors.
    val svd: SingularValueDecomposition[RowMatrix, Matrix] = mat.computeSVD(5, computeU = true)
    val U: RowMatrix = svd.U // The U factor is a RowMatrix.
    val s: Vector = svd.s // The singular values are stored in a local dense vector.
    val V: Matrix = svd.V // The V factor is a local dense matrix.

    log.info(s"####################singular values is $s")
    sc.stop()
  }
}
