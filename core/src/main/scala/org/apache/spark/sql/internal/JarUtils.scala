/*
 * Copyright (c) 2017-2019 TIBCO Software Inc. All rights reserved.
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

package org.apache.spark.sql.internal

import java.io.File
import java.net.{URL, URLClassLoader}
import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConverters._
import scala.collection.mutable

import com.pivotal.gemfirexd.internal.engine.Misc
import io.snappydata.sql.catalog.SnappyExternalCatalog

import org.apache.spark.Logging
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.analysis.NoSuchFunctionException
import org.apache.spark.sql.catalyst.catalog.CatalogFunction
import org.apache.spark.sql.collection.ToolsCallbackInit
import org.apache.spark.sql.execution.RefreshMetadata
import org.apache.spark.sql.{AnalysisException, SnappyContext}

/**
  * An utility class to store jar file reference with their individual class loaders.
  * This is to reflect class changes at driver side.
  * e.g. If an UDF definition changes the driver should pick up the correct UDF class.
  * This class can not initialize itself after a driver failure.
  * So the callers will have  to make sure that the classloader gets
  * initialized after a driver startup. Usually it can be achieved by
  * adding classloader at query time.
  *
  */
object ContextJarUtils extends Logging {
  val JAR_PATH = "snappy-jars"
  private val driverJars = new ConcurrentHashMap[String, URLClassLoader]().asScala
  val functionKeyPrefix = "__FUNC__"
  val droppedFunctionsKey = functionKeyPrefix + "DROPPED__"
  val DELIMITER = ","

  def addDriverJar(key: String, classLoader: URLClassLoader): Option[URLClassLoader] = {
    driverJars.putIfAbsent(key, classLoader)
  }

  def getDriverJar(key: String): Option[URLClassLoader] = driverJars.get(key)

  def removeDriverJar(key: String) : Unit = driverJars.remove(key)

  def getDriverJarURLs(): Array[URL] = {
    var urls = new mutable.HashSet[URL]()
    driverJars.foreach(_._2.getURLs.foreach(urls += _))
    urls.toArray
  }

  /**
    * This method will copy the given jar to Spark root directory
    * and then it will add the same jar to Spark.
    * This ensures class & jar isolation although it might be repetitive.
    *
    * @param prefix prefix to be given to the jar name
    * @param path   original path of the jar
    */
  def fetchFile(prefix: String, path: String): URL = {
    val callbacks = ToolsCallbackInit.toolsCallback
    val localName = path.split("/").last
    val changedFileName = s"${prefix}-${localName}"
    logInfo(s"Fetching jar $path to driver local directory $jarDir")
    val changedFile = new File(jarDir, changedFileName)
    if (!changedFile.exists()) {
      callbacks.doFetchFile(path, jarDir, changedFileName)
    }
    new File(jarDir, changedFileName).toURI.toURL
  }

  def deleteFile(prefix: String, path: String, isEmbedded: Boolean): Unit = {
    val callbacks = ToolsCallbackInit.toolsCallback
    if (callbacks != null) {
      val localName = path.split("/").last
      val changedFileName = s"${prefix}-${localName}"
      val jarFile = new File(jarDir, changedFileName)

      try {
        if (isEmbedded) {
          // Add to the list in (__FUNC__DROPPED__, dropped-udf-list)
          addToTheListInCmdRegion(droppedFunctionsKey, prefix + DELIMITER, droppedFunctionsKey)
        }
        if (jarFile.exists()) {
          jarFile.delete()
          RefreshMetadata.executeOnAll(sparkContext, RefreshMetadata.REMOVE_FUNCTION_JAR,
            Array(changedFileName))
        }
      } finally {
        if (isEmbedded) {
          Misc.getMemStore.getMetadataCmdRgn.remove(functionKeyPrefix + prefix)
        }
      }
    }
  }

  def removeFunctionArtifacts(externalCatalog: SnappyExternalCatalog,
      sessionCatalog: Option[SnappySessionCatalog], schemaName: String, functionName: String,
      isEmbeddedMode: Boolean, ignoreIfNotExists: Boolean = false): Unit = {
    val identifier = FunctionIdentifier(functionName, Some(schemaName))
    removeDriverJar(identifier.unquotedString)

    try {
      val catalogFunction = externalCatalog.getFunction(schemaName, identifier.funcName)
      catalogFunction.resources.foreach { r =>
        deleteFile(catalogFunction.identifier.toString(), r.uri, isEmbeddedMode)
      }
    } catch {
      case e: AnalysisException =>
        if (!ignoreIfNotExists) {
          sessionCatalog match {
            case Some(ssc) => ssc.failFunctionLookup(functionName)
            case None => throw new NoSuchFunctionException(schemaName, identifier.funcName)
          }
        } else { // Log, just in case.
          logDebug(s"Function ${identifier.funcName} possibly not found: $e")
        }
    }
  }

  def addFunctionArtifacts(funcDefinition: CatalogFunction, schemaName: String): Unit = {
    val k = funcDefinition.identifier.copy(database = Some(schemaName)).toString
    // resources has just one jar
    val jarPath = if (funcDefinition.resources.isEmpty) "" else funcDefinition.resources.head.uri
    Misc.getMemStore.getMetadataCmdRgn.put(ContextJarUtils.functionKeyPrefix + k, jarPath)
    // Remove from the list in (__FUNC__DROPPED__, dropped-udf-list)
    removeFromTheListInCmdRegion(ContextJarUtils.droppedFunctionsKey, k + ContextJarUtils.DELIMITER)
  }

  private def sparkContext = SnappyContext.globalSparkContext

  private def jarDir = {
    val jarDirectory = new File(System.getProperty("user.dir"), JAR_PATH)
    if (!jarDirectory.exists()) jarDirectory.mkdir()
    jarDirectory
  }

  def addToTheListInCmdRegion(k: String, item: String, head: String): Unit = {
    val r = Misc.getMemStore.getMetadataCmdRgn
    var old1: String = null
    var old2: AnyRef = null
    do {
      val oldObj = r.get(k)
      old1 = if ( oldObj != null ) oldObj.asInstanceOf[String] else null
      val newValue = if (old1 != null) old1 + item else head + item
      old2 = r.put(k, newValue)
    } while (old1 != old2)
  }

  def removeFromTheListInCmdRegion(k: String, item: String): Unit = {
    val r = Misc.getMemStore.getMetadataCmdRgn
    var old1: String = null
    var old2: AnyRef = null
    do {
      val oldObj = r.get(k)
      if (oldObj != null) {
        old1 = oldObj.asInstanceOf[String]
        val newValue = old1.replace(item, "")
        old2 = r.put(k, newValue)
      }
    } while (old1 != old2.asInstanceOf[String])
  }

  def checkItemExists(k: String, item: String): Boolean = {
    var value = Misc.getMemStore.getMetadataCmdRgn.get(k)
    if (value != null) {
      val valueStr = value.asInstanceOf[String]
      return valueStr.contains(item)
    }
    false
  }
}


