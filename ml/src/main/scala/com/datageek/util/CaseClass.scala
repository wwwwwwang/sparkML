package com.datageek.util

case class PredictAndLabel(key_id: String,
                           rawLabel: Double,
                           newLabel: Double,
                           uniqueId: String,
                           timestamp: String)

case class PredictOnly(key_id: String,
                        predictLabel: Double,
                        uniqueId: String,
                        timestamp: String)

case class FPGItems(key_id: String,
                       items: String,
                       freq: Long,
                        timestamp: String)

case class TrainResult(key_id: String,
                       message: String,
                       timestamp: String)

case class FPGRules(key_id: String,
                      antecedent: String,
                      consequent : String,
                      confidence: Double,
                      timestamp: String)

case class ApplicationId(jobId: String,
                        appName: String,
                        appId: String,
                        timestamp: String)

case class ModeSavPath(jobId: String,
                       appName: String,
                       savePath: String,
                       timestamp: String)
