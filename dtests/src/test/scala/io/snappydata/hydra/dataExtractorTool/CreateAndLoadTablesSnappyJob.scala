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

import java.io.PrintWriter

import scala.util.{Failure, Success, Try}

import com.typesafe.config.Config
import org.apache.spark.sql.{SnappyJobValid, SnappyJobValidation, SnappySQLJob, SnappySession, _}

object CreateAndLoadTablesSnappyJob extends SnappySQLJob {
  // scalastyle:off println
  println(" Inside CreateAndLoadSnappyJob")
  override def runSnappyJob(snSession: SnappySession, jobConfig: Config): Any = {
    val snc = snSession.sqlContext
    val dataFilesLocation = jobConfig.getString("dataFilesLocation")
    val queryFile: String = jobConfig.getString("queryFile")
    val queryArray = scala.io.Source.fromFile(queryFile).getLines().mkString.split(";")
    val expectedCntWithGrant: Int = jobConfig.getInt("expectedCntWithGrant")
    val unExpectedCntWithGrant: Int = jobConfig.getInt("unExpectedCntWithGrant")

    def getCurrentDirectory = new java.io.File(".").getCanonicalPath

    // scalastyle:off println
    val pw = new PrintWriter("SnappyJobOutPut.out")
    Try {
      // DataExtractorTestUtil.createColRowTables(snc, dataFilesLocation)
      DataExtractorTestUtil.runQueries(snc, queryArray, expectedCntWithGrant,
          unExpectedCntWithGrant, pw)
        }
    match {
      case Success(v) => pw.close()
        s"See ${getCurrentDirectory}/SnappyJobOutPut.out"
      case Failure(e) => pw.close();
        throw e;
     }
  }

  override def isValidJob(sc: SnappySession, config: Config): SnappyJobValidation = SnappyJobValid()
}
