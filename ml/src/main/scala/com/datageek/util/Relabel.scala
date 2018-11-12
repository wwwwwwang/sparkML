package com.datageek.util

import org.apache.spark.rdd.RDD

object Relabel {
  def getValue(m: Map[Double, Double], key: Double): Double = {
    m.getOrElse(key, 0)
  }

  def getMap(labels: RDD[(Double, Double)], num: Int): Map[Double, Double] = {
    var res = Map[Double, Double]()
    var rows = List[Double]()
    var cols = List[Double]()
    val a = labels.map { case (l, p) => ((l, p), 1.0) }.reduceByKey(_ + _).sortBy(_._2, ascending = false)
      .collect.toList
    var row = -1.0
    var col = -1.0
    for (j <- a.indices) {
      row = a(j)._1._1
      col = a(j)._1._2
      if (!rows.contains(row) && !cols.contains(col)) {
        rows = rows.::(row)
        cols = cols.::(col)
        res += (col -> row)
      }
    }
    res
  }

  def run1(predictionAndLabel:RDD[(String,Double,Double,String)], num: Int): RDD[(String,Double,Double,String)] = {
    val labels = predictionAndLabel.map(a=>(a._2,a._3))
    val labelMap = getMap(labels, num)
    //println(s"whsh----------->labelMap.size = ${labelMap.size}, labelMap = ${labelMap.toString()}")
    predictionAndLabel.map(a=>(a._1,a._2,getValue(labelMap, a._3),a._4))
  }

  def run(labels: RDD[(Double, Double)], num: Int): RDD[(Double, Double)] = {
    val labelMap = getMap(labels, num)
    println(s"whsh----------->labelMap.size = ${labelMap.size}, labelMap = ${labelMap.toString()}")
    labels.map(a => (a._1, getValue(labelMap, a._2)))
  }

}
