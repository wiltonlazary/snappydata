/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.sql.execution.oplog

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, UnsafeRow}
import org.apache.spark.sql.execution.LeafExecNode
import org.apache.spark.sql.execution.columnar.impl.BaseColumnFormatRelation
import org.apache.spark.sql.sources.BaseRelation


class OpLogTableScan(
                      rdd: RDD[Any],
                      relation: BaseRelation,
                      schemaAttributes: Seq[AttributeReference],
                      session: SparkSession) extends LeafExecNode {

  override protected def doExecute(): RDD[InternalRow] = {
    val rddOfInternalRow = rdd.map(pair => {
      println(s"1891: from oplogtablescan doexecute")
      // val p = pair.asInstanceOf[Seq[Integer]]
      val ur = new UnsafeRow(2)
      val data = new Array[Byte](18)

      ur.pointTo(data, 18)
      ur.setInt(0, 100)
      ur.setInt(1, 111)
      ur.asInstanceOf[InternalRow]
      // new GenericInternalRow(r).asInstanceOf[InternalRow]
    })
    rddOfInternalRow
  }


  override def output: Seq[Attribute] = schemaAttributes

  override def productElement(n: Int): Any = {
    null
  }

  override def productArity: Int = {
    1
  }

  override def canEqual(that: Any): Boolean = {
    false
  }
}
