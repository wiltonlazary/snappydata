/*
 * Copyright (c) 2018 SnappyData, Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */
package io.snappydata.hydra.dataExtractorTool

import java.io.{File, FileOutputStream, PrintWriter}

import io.snappydata.hydra.northwind.{NWQueries, NWTestUtil}

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SnappyContext

object DataExtractorSparkApp {

  def main(args: Array[String]) {
    // scalastyle:off println
    Thread.sleep(60000L)
    val connectionURL = args(args.length - 1)
    println("The connection url is " + connectionURL)
    val conf = new SparkConf().
        setAppName("CreateAndLoadNWTablesSpark Application").
        set("snappydata.connection", connectionURL)
    val sc = SparkContext.getOrCreate(conf)
    val snc = SnappyContext(sc)

    val pw = new PrintWriter(new FileOutputStream(new File("CreateAndLoadNWTablesSparkApp.out"),
      true));
    println("Getting users arguments")
    val queryFile = args(1)
    val queryArray = scala.io.Source.fromFile(queryFile).getLines().mkString.split(";")
    val expectedExcptCnt = args(2).toInt
    val unExpectedExcptCnt = args(3).toInt
    DataExtractorTestUtil.runQueries(snc, queryArray, expectedExcptCnt, unExpectedExcptCnt, pw)
    pw.close()
  }
}

