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
package org.apache.spark.sql.execution.oplog.impl

import scala.collection.mutable

import io.snappydata.Constant
import io.snappydata.recovery.RecoveryService

import org.apache.spark.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.sources.{BaseRelation, TableScan}
import org.apache.spark.sql.types.StructType

class OpLogFormatRelation(
    fqtn: String,
    _schema: StructType,
    partitioningColumns: Seq[String],
    context: SQLContext,
    options: Map[String, String]) extends BaseRelation with TableScan with Logging {
  val schema: StructType = _schema
  val schemaName: String = fqtn.split('.')(0)
  val tableName: String = fqtn.split('.')(1)

  def columnBatchTableName(): String = {
    schemaName + '.' + Constant.SHADOW_SCHEMA_NAME_WITH_SEPARATOR +
        tableName + Constant.SHADOW_TABLE_SUFFIX
  }

  def scnTbl(externalColumnTableName: String, requiredColumns: Array[String],
      filters: Array[Expression], prunePartitions: () => Int): (RDD[Any], Array[Int]) = {

    val projection = null
    val provider = RecoveryService.getProvider(fqtn)
    val snappySession = SparkSession.builder().getOrCreate().asInstanceOf[SnappySession]

    val tableSchemas = RecoveryService.schemaStructMap
    val versionMap = RecoveryService.versionMap
    val tableColIdsMap = RecoveryService.tableColumnIds
    val bucketHostMap: mutable.Map[Int, String] = mutable.Map.empty
    val catalogTable = snappySession.externalCatalog.getTable(schemaName, tableName)
    val primaryKeys = catalogTable.properties.getOrElse("primary_keys", "")
    val keyColumns = options.getOrElse("key_columns", "")
    val schemaLowerCase = StructType(schema.map(f => f.copy(name = f.name.toLowerCase)))
    val removePattern = "(executor_).*(_)".r

    (0 until RecoveryService.getNumBuckets(schemaName.toUpperCase, tableName.toUpperCase)._1).foreach(i => {
      bucketHostMap.put(i,
        removePattern.replaceAllIn(RecoveryService.getExecutorHost(fqtn.toUpperCase(), i).head, ""))
    })

    (new OpLogRdd(snappySession, fqtn, externalColumnTableName, schemaLowerCase,
      partitioningColumns, provider, projection, filters, (filters eq null) || filters.length == 0,
      prunePartitions, tableSchemas, versionMap, tableColIdsMap, bucketHostMap, primaryKeys, keyColumns), projection)
  }


  override def sqlContext: SQLContext = context

  override def buildScan(): RDD[Row] = {
    val externalColumnTableName = columnBatchTableName()
    scnTbl(externalColumnTableName, null, null, null)._1.asInstanceOf[RDD[Row]]
  }

}
