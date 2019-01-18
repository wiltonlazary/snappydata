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

import java.io.{File, PrintWriter}

import org.apache.spark.sql.SnappyContext

object DataExtractorTestUtil {
  // scalastyle:off println
  var expectedExpCnt = 0;
  var unExpectedExpCnt = 0;

  def createColRowTables(snc: SnappyContext, dataLocation: String) : Unit = {
    println("Inside createColAndRow tables")
    val fileExtensions = List("csv")
    val files = getListOfFiles(new File(dataLocation), fileExtensions)
    System.out.println("The number of files are " + files.size)
    for (file <- files) {
      val hfile = file.toString()
      val filename = file.getName()
      System.out.println("The file name is " + filename)
      System.out.println("The hfile name is " + hfile)
      val df = snc.read
          .format("com.databricks.spark.csv") // CSV to DF package
          .option("header", "true")
          .option("inferSchema", "true")
          .option("nullValue", "NULL")
          .option("maxCharsPerColumn", "4096")
          .load(hfile)
      val tableName = filename.split("\\.")(0)
      System.out.println("Table name is " + tableName)
      df.write.insertInto(tableName)
    }
  }

  def getListOfFiles(dir: File, extensions: List[String]): List[File] = {
    dir.listFiles.filter(_.isFile).toList.filter { file =>
      extensions.exists(file.getName.endsWith(_))
    }
  }

  def runQueries(snc: SnappyContext, queryArray: Array[String], expectExcpCnt: Integer,
      unExpectExcpCnt: Integer, pw: PrintWriter)
  : Unit = {
    println("Inside run/queries inside SecurityUtil")
    try {
      for (i <- 0 to queryArray.length - 1) {
        snc.sql(queryArray(i)).show
      }
    }
    catch {
      case ex: Exception => {
        if (ex.getMessage().contains("SELECT")) {
          unExpectedExpCnt = unExpectedExpCnt + 1
          println("Got unExpected Exception " + ex.printStackTrace())
          println("unExpectedExpCnt = " + unExpectedExpCnt)
        }
        else if (ex.getMessage().contains("INSERT") || ex.getMessage().contains("UPDATE") ||
            ex.getMessage().contains("DELETE") || ex.getMessage().contains("PUTINTO")){
          expectedExpCnt = expectedExpCnt + 1
          println("Got Expected exception " + ex.printStackTrace())
          println("expectedCnt = " + expectedExpCnt)
        }
      }
    }
    validate(expectExcpCnt, unExpectExcpCnt)
    }

  def validate(expectedCnt: Integer, unExpectedCnt: Integer): Unit = {
    if (unExpectedCnt == unExpectedExpCnt) {
      println("Validation SUCCESSFUL Got expected cnt of unExpectedException = " + unExpectedCnt)

    }
    else {
      sys.error("Validation failure expected cnt was = " + unExpectedCnt + " but got = "
          + unExpectedCnt)
    }
    if (expectedCnt == expectedExpCnt) {
      println("Validation SUCCESSFUL Got expected cnt of expectedException = " + expectedCnt)

    }
    else {
      sys.error("Validation failure expected cnt was = " + expectedCnt + " but got = "
          + expectedExpCnt)
    }
    unExpectedExpCnt = 0;
    expectedExpCnt = 0;
  }
}
