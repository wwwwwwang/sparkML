package com.datageek.util

object MakeCategoricalFeatures {
  def make(s: String): Map[Int, Int] = {
    var categoricalFeaturesInfo = Map[Int, Int]()
    if (!s.equalsIgnoreCase("") && s != null) {
      if (s.contains(",")) {
        val array = s.split(",")
        array.foreach(a => {
          if (a.contains(":")) {
            val res = a.split(":")
            categoricalFeaturesInfo += (res(0).toInt -> res(1).toInt)
          }
        })
      } else {
        if (s.contains(":")) {
          val res = s.split(":")
          categoricalFeaturesInfo += (res(0).toInt -> res(1).toInt)
        }
      }
    }
    categoricalFeaturesInfo
  }
}
