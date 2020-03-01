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
package io.snappydata.cluster

import java.io.{BufferedOutputStream, BufferedWriter, ByteArrayOutputStream, File, FileWriter,
  PrintStream, PrintWriter}
import java.sql.{Connection, DriverManager, ResultSet, Statement, Timestamp}
import java.util.Properties

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.sys.process.{Process, ProcessLogger, stderr, stdout, _}
import scala.util.Try
import scala.util.control.NonFatal

import com.pivotal.gemfirexd.Attribute
import com.pivotal.gemfirexd.Property.{AUTH_LDAP_SEARCH_BASE, AUTH_LDAP_SERVER}
import com.pivotal.gemfirexd.security.{LdapTestServer, SecurityTestUtils}
import io.snappydata.test.dunit.{AvailablePortHelper, DistributedTestBase}
import io.snappydata.thrift.internal.{ClientBlob, ClientClob}
import org.apache.commons.io.output.TeeOutputStream
import org.scalatest.Assertions._

import org.apache.spark.Logging
import org.apache.spark.sql.collection.Utils
import org.apache.spark.sql.udf.UserDefinedFunctionsDUnitTest

class PrimaryDUnitRecoveryTest(s: String) extends DistributedTestBase(s)
    with Logging {

  val adminUser1 = "gemfire10"
  private val locatorNetPort = AvailablePortHelper.getRandomAvailableTCPPort
  private var confDirPath = ""
  private var workDirPath = ""
  private val recovery_mode_dir = System.getProperty("RECOVERY_TEST_DIR")
  private var test_status: Boolean = false


  def clearDirectory(dir: File): Unit = {
    if (dir.isDirectory) {
      dir.listFiles().foreach(clearDirectory)
    }
    if (dir.exists() && !dir.delete()) {
      throw new Exception("Error clearing Directory/File" + dir.getAbsolutePath)
    }
  }

  /**
   * start LDAP server in beforeAll
   */
  override def beforeClass(): Unit = {

    PrimaryDUnitRecoveryTest.snappyHome = System.getenv("SNAPPY_HOME")
    // start LDAP server
    logInfo("Starting LDAP server")
    // starts LDAP server and sets LDAP properties to be passed to conf files
    setSecurityProps()
  }

  def setSecurityProps(): Unit = {
    import com.pivotal.gemfirexd.Property.{AUTH_LDAP_SEARCH_BASE, AUTH_LDAP_SERVER}

    PrimaryDUnitRecoveryTest.ldapProperties =
        SecurityTestUtils.startLdapServerAndGetBootProperties(0, 0,
          adminUser1, getClass.getResource("/auth.ldif").getPath)
    for (k <- List(Attribute.AUTH_PROVIDER, AUTH_LDAP_SERVER, AUTH_LDAP_SEARCH_BASE)) {
      System.setProperty(k, PrimaryDUnitRecoveryTest.ldapProperties.getProperty(k))
    }
  }

  override def afterClass(): Unit = {
    // 1. stop  ldap cluster.
    stopLdapTestServer()
    // 2. delete all
  }

  def stopLdapTestServer(): Unit = {
    val ldapServer = LdapTestServer.getInstance()
    if (ldapServer.isServerStarted) {
      ldapServer.stopService()
    }
  }

  def afterEach(): Unit = {
    val confDir = new File(confDirPath)
    val workDir = new File(workDirPath)

    stopCluster()
    if (test_status) {
      logInfo("Clearing conf and work dir.")
      clearDirectory(confDir)
      clearDirectory(workDir)
    }
    confDirPath = ""
    workDirPath = ""
    test_status = false
  }

  def startSnappyCluster(): Unit = {
    val (out, _) =
      PrimaryDUnitRecoveryTest.executeCommand(s"${PrimaryDUnitRecoveryTest.snappyHome}" +
        s"/sbin/snappy-start-all.sh --config $confDirPath")

    // TODO need a better way to ensure the cluster has started
    if (!out.contains("Distributed system now")) {
      throw new Exception(s"Failed to start Snappy cluster.")
    }
  }


  def basicOperationSetSnappyCluster(stmt: Statement, defaultSchema: String = "APP"): Unit = {
    // covers case: data only in row buffers
    stmt.execute(
      s"""
        CREATE TABLE $defaultSchema.test1coltab1 (
          col1 Int, col2 String, col3 Decimal
        ) USING COLUMN OPTIONS (buckets '1', COLUMN_MAX_DELTA_ROWS '4')""")
    stmt.execute(s"INSERT INTO $defaultSchema.test1coltab1 values(1,'aaaa',2.2)")
    stmt.execute(s"INSERT INTO $defaultSchema.test1coltab1 values(2,'bbbb',3.3)")

    stmt.execute(s"CREATE VIEW $defaultSchema.vw_test1coltab1 AS " +
        s"(SELECT * FROM $defaultSchema.test1coltab1)")
    stmt.execute("CREATE SCHEMA tapp")

    // empty column table & not null column
    // covers - empty buckets
    stmt.execute("CREATE TABLE tapp.test1coltab1" +
        " (col1 int, col2 int NOT NULL, col3 varchar(22) NOT NULL)" +
        " USING COLUMN OPTIONS (BUCKETS '5', COLUMN_MAX_DELTA_ROWS '10')")

    // empty row table & not null column
    stmt.execute("CREATE TABLE tapp.test1rowtab3 (col1 int, col2 int NOT NULL, col3 varchar(22))" +
        " USING ROW OPTIONS(PARTITION_BY 'col1', buckets '1')")

    stmt.execute("CREATE DISKSTORE anotherdiskstore ('./testDS' 10240)")

    stmt.execute(s"CREATE TABLE $defaultSchema.test1coltab4 (col1 int NOT NULL," +
        " col2 int not null) USING COLUMN OPTIONS(diskstore 'anotherdiskstore')")
    stmt.execute(s"INSERT INTO $defaultSchema.test1coltab4 values(11,111)")
    stmt.execute(s"INSERT INTO $defaultSchema.test1coltab4 values(333,33)")
    stmt.execute(s"INSERT INTO $defaultSchema.test1coltab4 values(11,111)")
    stmt.execute(s"INSERT INTO $defaultSchema.test1coltab4 values(333,33)")

    // covers few empty buckets case
    stmt.execute(s"CREATE TABLE $defaultSchema.test1rowtab5 (col1 int NOT NULL," +
        " col2 String not null) using row" +
        " options(partition_by 'col1', buckets '22', diskstore 'anotherdiskstore')")
    stmt.execute(s"INSERT INTO $defaultSchema.test1rowtab5 values(111,'adsf')")
    stmt.execute(s"INSERT INTO $defaultSchema.test1rowtab5 values(2223,'zxcvxcv')")
    stmt.execute(s"INSERT INTO $defaultSchema.test1rowtab5 values(111,'adsf')")
    stmt.execute(s"INSERT INTO $defaultSchema.test1rowtab5 values(2223,'zxcvxcv')")
    stmt.execute(s"INSERT INTO $defaultSchema.test1rowtab5 values(111,'adsf')")
    stmt.execute(s"INSERT INTO $defaultSchema.test1rowtab5 values(2223,'zxcvxcv')")


    val udfText: String = "public class IntegerUDF implements " +
        "org.apache.spark.sql.api.java.UDF1<String,Integer> {" +
        " @Override public Integer call(String s){ " +
        "               return 6; " +
        "}" +
        "}"
    val file = UserDefinedFunctionsDUnitTest.createUDFClass("IntegerUDF", udfText)
    val jar = UserDefinedFunctionsDUnitTest.createJarFile(Seq(file))

    stmt.execute(s"CREATE FUNCTION $defaultSchema.intudf1 AS IntegerUDF " +
        s"RETURNS Integer USING JAR " +
        s"'$jar'")
    stmt.execute(s"CREATE FUNCTION $defaultSchema.intudf2 AS IntegerUDF " +
        s"RETURNS Integer USING JAR " +
        s"'$jar'")
    stmt.execute(s"CREATE FUNCTION $defaultSchema.intudf3 AS IntegerUDF " +
        s"RETURNS Integer USING JAR " +
        s"'$jar'")
    stmt.execute(s"drop function $defaultSchema.intudf2")

    // nulls in data - row table
    stmt.execute(s"CREATE TABLE $defaultSchema.test1rowtab6 (col1 int, col2 string, col3 float," +
        s" col4 short, col5 boolean) using row")
    stmt.execute(s"INSERT INTO $defaultSchema.test1rowtab6 VALUES(null,'adsf',null, 12, 0)")
    stmt.execute(s"INSERT INTO $defaultSchema.test1rowtab6 values" +
        s" (null,'xczadsf',232.1222, 11, null)")
    stmt.execute(s"INSERT INTO $defaultSchema.test1rowtab6 values" +
        s" (null,null,333.333, null, 'true')")

    // data only in column batches - not in row buffers - nulls in data - column table
    stmt.execute(s"CREATE TABLE $defaultSchema.test1coltab7 (col1 Bigint, col2 char(2), col3" +
        s" double,col4 byte,col5 date)USING COLUMN OPTIONS(buckets '2',COLUMN_MAX_DELTA_ROWS '3')")
    stmt.execute(s"INSERT INTO $defaultSchema.test1coltab7" +
        s" VALUES(9123372036812312307, 'aa',123.123324, 12,'2019-03-20')")
    stmt.execute(s"INSERT INTO $defaultSchema.test1coltab7 " +
        s"VALUES(null, 'bb',345.123324, 11,'2019-03-21')")
    stmt.execute(s"INSERT INTO $defaultSchema.test1coltab7 " +
        s"VALUES(8123372036812312307, 'cc',null, null, null)")

    // data in only row buffer of column table - nulls in data
    stmt.execute(s"CREATE TABLE $defaultSchema.test1coltab8 (col1 Bigint, col2 varchar(44), col3" +
        s" double,col4 byte,col5 date)USING COLUMN OPTIONS(BUCKETS '5',COLUMN_MAX_DELTA_ROWS '4')")
    stmt.execute(s"INSERT INTO $defaultSchema.test1coltab8 VALUES(9123372036812312307," +
        s" null,123.123324, 12,null)")
    stmt.execute(s"INSERT INTO $defaultSchema.test1coltab8 VALUES(8123372036812312307," +
        s" 'qewrwr4',345.123324, 11,'2019-03-21')")
    stmt.execute(s"INSERT INTO $defaultSchema.test1coltab8 VALUES(null, null,null, null, null)")

    writeToFile("1,aaaa,11.11\n2,bbbb,222.2\n333,ccccc,333.33", "/tmp/test1_exttab1.csv")
    stmt.execute("create external table test1_exttab1 using csv" +
        " options(path '/tmp/test1_exttab1.csv')")
  }

  def stopCluster(): Unit = {
    // TODO need a way to ensure the cluster has stopped
    PrimaryDUnitRecoveryTest.executeCommand(s"${PrimaryDUnitRecoveryTest.snappyHome}" +
        s"/sbin/snappy-stop-all.sh --config $confDirPath")
  }

  def startSnappyRecoveryCluster(): Unit = {
    val (out, _) =
      PrimaryDUnitRecoveryTest.executeCommand(s"${PrimaryDUnitRecoveryTest.snappyHome}" +
        s"/sbin/snappy-start-all.sh --recover --config $confDirPath")
    // TODO need a better way to ensure the cluster has started
    if (!out.contains("Distributed system now")) {
      throw new Exception(s"Failed to start Snappy cluster in recovery mode.")
    }
  }

  def createConfDir(testName: String): String = {
    val confdir = createFileDir(recovery_mode_dir + File.separator + "conf_" + testName)
    confdir.getAbsolutePath
  }

  def createFileDir(absDirPath: String): File = {
    val dir = new File(absDirPath)
    if (!dir.exists()) {
      dir.mkdir()
    }
    if (!dir.exists()) {
      throw new Exception("Error creating custom work directory at " + recovery_mode_dir)
    }
    dir
  }

  def createDir(fileName: String): String = {
    val f = new File(fileName)
    f.mkdir()
    f.deleteOnExit()
    fileName
  }

  def createWorkDir(testName: String, leadsNum: Int, locatorsNum: Int, serversNum: Int): String = {
    // work dir
    val workDir = createFileDir(recovery_mode_dir + File.separator + "work_" + testName)
    // leads dir inside work dir
    for (i: Int <- 1 to leadsNum) {
      createDir(recovery_mode_dir + File.separator +
          "work_" + testName + File.separator + s"lead-$i")
    }
    // locators dir inside work dir
    for (i <- 1 to locatorsNum) {
      createDir(recovery_mode_dir + File.separator +
          "work_" + testName + File.separator + s"locator-$i")
    }
    // servers dir inside work dir
    for (i <- 1 to serversNum) {
      createDir(recovery_mode_dir + File.separator +
          "work_" + testName + File.separator + s"server-$i")
    }
    workDir.getAbsolutePath
  }

  def renameLater(resultSet: ResultSet, colCount: Int, stringBuilder: StringBuilder,
      filePathOrg: String): mutable.StringBuilder = {
    while (resultSet.next()) {
      stringBuilder.clear()
      (1 until colCount).foreach(i => {
        resultSet.getObject(i) match {
          case clob: ClientClob => stringBuilder ++=
              s"${clob.getSubString(1L, clob.length().toInt)},"
          case _ => stringBuilder ++= s"${resultSet.getObject(i)},"
        }
      })
      resultSet.getObject(colCount) match {
        case clob: ClientClob => stringBuilder ++=
            s"${
              clob.getSubString(1L, clob.length().toInt)
            }"
        case _ => stringBuilder ++= s"${resultSet.getObject(colCount)}"
      }
      // todo: can be improved using batching 100 rows
      writeToFile(stringBuilder.toString(), filePathOrg, true)
    }
    null
  }

  def compareResultSet(fqtn: String, resultSet: ResultSet, isRecoveredDataRS: Boolean): Unit = {
    val tableName = fqtn.replace(".", "_")
    val dir = new File(workDirPath + File.separator + tableName)
    if (!dir.exists() && !isRecoveredDataRS) {
      dir.mkdir()
    }
    if (isRecoveredDataRS && !dir.exists()) {
      // Since the directory is created every time in regular mode and re-used
      // in recovery mode and deleted after the comparison.
      throw new Exception(s"Directory ${dir.getAbsoluteFile} for table:" +
          s" $tableName is expected to exist.")
    }
    val stringBuilder = new mutable.StringBuilder()
    val filePathOrg = dir.getAbsoluteFile + File.separator + tableName + "_ORG.txt"
    val filePathRec = dir.getAbsoluteFile + File.separator + tableName + "_RECOVERED.txt"
    if (!isRecoveredDataRS) {
      val colCount = resultSet.getMetaData.getColumnCount
      while (resultSet.next()) {
        stringBuilder.clear()
        (1 until colCount).foreach(i => {
          resultSet.getObject(i) match {
            case clob: ClientClob => {
              stringBuilder ++= s"${
                clob
                    .getSubString(1L, clob.length().toInt)
              },"
            }
            case blob: ClientBlob => {
              stringBuilder ++= s"${
                scala.io.Source.fromInputStream(resultSet.getBlob(i).getBinaryStream).mkString
              },"
            }
            case _ =>
              stringBuilder ++= s"${resultSet.getObject(i)},"
          }
        })
        resultSet.getObject(colCount) match {
          case clob: ClientClob => {
            stringBuilder ++= s"${
              clob.getSubString(1L, clob.length().toInt)
            }"
          }
          case blob: ClientBlob => {
            stringBuilder ++= s"${
              scala.io.Source.fromInputStream(resultSet.getBlob(colCount).getBinaryStream).mkString
            }"
          }
          case _ =>
            stringBuilder ++= s"${resultSet.getObject(colCount)}"
        }
        // todo: can be improved using batching 100 rows
        writeToFile(stringBuilder.toString(), filePathOrg, true)
      }
    } else {
      val colCount: Int = resultSet.getMetaData.getColumnCount
      while (resultSet.next()) {
        stringBuilder.clear()
        (1 until colCount).foreach(i => {
          resultSet.getObject(i) match {
            case clob: ClientClob => {
              stringBuilder ++= s"${
                clob.getSubString(1L, clob.length().toInt)
              },"
            }
            case blob: ClientBlob => {
              stringBuilder ++= s"${
                scala.io.Source.fromInputStream(resultSet.getBlob(i).getBinaryStream).mkString
              },"
            }
            case _ =>
              stringBuilder ++= s"${resultSet.getObject(i)},"
          }
        })
        resultSet.getObject(colCount) match {
          case clob: ClientClob => {
            stringBuilder ++= s"${
              clob.getSubString(1L, clob.length().toInt)
            }"
          }
          case blob: ClientBlob => {
            stringBuilder ++= s"${
              scala.io.Source.fromInputStream(resultSet.getBlob(colCount).getBinaryStream).mkString
            }"
          }
          case _ =>
            stringBuilder ++= s"${resultSet.getObject(colCount)}"
        }

        // todo: can be improved using batching 100 rows
        writeToFile(stringBuilder.toString(), filePathRec, true)
      }
      val cmd = s"comm --nocheck-order -3 $filePathOrg $filePathRec"
      var diffRes: String = ""
      Try {
        diffRes = cmd.!! // todo won't work on windows. Should be done in code.!?
      } match {
        case scala.util.Success(_) => assert(diffRes.isEmpty, s"\nRecovered data does not" +
            s" match the original data.\nOrginal data is present in $filePathOrg\n " +
            s"Recovered data is present in $filePathRec")
          // delete the directory after the job is done.
          dir.listFiles().foreach(file => file.delete())
          if (dir.listFiles().length == 0) dir.delete()

        case scala.util.Failure(exception) =>
          logInfo(s"Error comparing output files.\n$exception")
      }
    }
  }


  def writeToFile(str: String, filePath: String, append: Boolean = false): Unit = {
    var pw: PrintWriter = null
    if (append) {
      val fileWriter = new FileWriter(filePath, append)
      val bufferedWriter = new BufferedWriter(fileWriter)
      pw = new PrintWriter(bufferedWriter)
      pw.println(str)
      pw.close()
      bufferedWriter.close()
      fileWriter.close()
    } else {
      pw = new PrintWriter(filePath)
      pw.write(str)
      pw.flush()
      pw.close()
      // wait until file becomes available (e.g. running on NFS)
      var matched = false
      while (!matched) {
        Thread.sleep(100)
        try {
          val source = scala.io.Source.fromFile(filePath)
          val lines = try {
            source.mkString
          } finally {
            source.close()
          }
          matched = lines == str
        } catch {
          case NonFatal(_) =>
        }
      }
    }
  }

  def getConn(port: Int, user: String = "", password: String = ""): Connection = {
    val driver = "io.snappydata.jdbc.ClientDriver"
    val url: String = "jdbc:snappydata://localhost:" + port + "/"
    Utils.classForName(driver).newInstance
    if (user.isEmpty && password.isEmpty) {
      DriverManager.getConnection(url)
    } else {
      DriverManager.getConnection(url, user, password)
    }
  }

  // todo: add a test to test compareTo - see if sorted set's order is correct
  // todo: ... also cover each case in the if else ladder in the method.


  //  test("test1 - Basic test to check commands like describe, show, procedures " +
  //      "and list tables names, schemas names and UDFs using LDAP") {
  def test1(): Unit = {
    try {
      // set separate work directory and conf directory
      confDirPath = createConfDir("test1");
      val leadsNum = 1
      val locatorsNum = 1
      val serversNum = 1
      workDirPath = createWorkDir("test1", leadsNum, locatorsNum, serversNum)

      // 3. create conf files with required configuration, as required by the test,
      // inside the Conf dir - also mention the new work dir as a config

      val waitForInit = "-jobserver.waitForInitialization=true"
      val locatorPort = AvailablePortHelper.getRandomAvailableUDPPort

      val locNetPort = locatorNetPort
      val netPort2 = AvailablePortHelper.getRandomAvailableTCPPort
      val netPort3 = AvailablePortHelper.getRandomAvailableTCPPort
      val ldapConf = PrimaryDUnitRecoveryTest.getLdapConf
      writeToFile(s"localhost  -peer-discovery-port=$locatorPort -dir=$workDirPath/locator-1" +
          s" -client-port=$locNetPort $ldapConf", s"$confDirPath/locators")
      writeToFile(s"localhost  -locators=localhost[$locatorPort]  -dir=$workDirPath/lead-1" +
          s" $waitForInit $ldapConf", s"$confDirPath/leads")
      writeToFile(
        s"""localhost  -locators=localhost[$locatorPort] -dir=$workDirPath/server-1 -client-port=$netPort2 $ldapConf
           |""".stripMargin, s"$confDirPath/servers")

      startSnappyCluster()

      val conn = getConn(locNetPort, "gemfire10", "gemfire10")
      val stmt = conn.createStatement()
      basicOperationSetSnappyCluster(stmt, "gemfire10")

      stmt.close()
      conn.close()

      stopCluster()

      startSnappyRecoveryCluster()
      // todo: Resolve bug - Table is not found if queried immediately after
      Thread.sleep(5000)
      // TODO: Add cases that fail
      // TODO: Add test case for sample tables

      val connRec = getConn(locNetPort, "gemfire10", "gemfire10")
      val stmtRec = connRec.createStatement()
      // reused below multiple times; clear before using str
      var str: StringBuilder = new StringBuilder
      var tempTab = ""
      val arrBuf: ArrayBuffer[String] = ArrayBuffer.empty
      var i = 0

      def resetBuffer = {
        arrBuf.clear()
        i = 0
      }

      var rs = stmtRec.executeQuery("SELECT * FROM gemfire10.test1coltab1 ORDER BY col1")
      logDebug("=== SELECT * FROM test1coltab1 ===\n")
      str.clear()
      resetBuffer
      arrBuf ++= ArrayBuffer("1,aaaa,2.2", "2,bbbb,3.3")
      while (rs.next()) {
        assert((s"${rs.getInt("col1")}," +
            s"${rs.getString("col2")},${rs.getFloat("col3")}").equalsIgnoreCase(arrBuf(i)))
        i += 1
      }
      rs.close()

      rs = stmtRec.executeQuery("SELECT * FROM tapp.test1coltab1")
      logDebug("SELECT * FROM tapp.test1coltab1")
      str.clear()
      while (rs.next()) {
        str ++= s"${rs.getInt(2)}\t"
      }
      assert(str.toString().length === 0) // empty table
      rs.close()


      rs = stmtRec.executeQuery("SELECT * FROM tapp.test1rowtab3")
      logDebug("SELECT * FROM tapp.test1rowtab3")
      str.clear()
      while (rs.next()) { // should not go in the loop as the table is empty.
        str ++= s"${rs.getInt(2).toString}"
      }
      assert(str.toString().length === 0)
      rs.close()

      rs = stmtRec.executeQuery("show tables in gemfire10")
      logDebug("TableNames in gemfire10:\n")
      str.clear()
      while (rs.next()) {
        tempTab = rs.getString("tableName") + " "
        logDebug(tempTab)
        str ++= tempTab
      }
      val tempStr = str.toString().toUpperCase()
      // find better way to assert this case
      assert(tempStr.contains("TEST1COLTAB1") &&
          tempStr.contains("TEST1COLTAB4") &&
          tempStr.contains("TEST1COLTAB7") &&
          tempStr.contains("TEST1COLTAB8") &&
          tempStr.contains("TEST1ROWTAB5") &&
          tempStr.contains("TEST1ROWTAB6") &&
          tempStr.contains("VW_TEST1COLTAB1")
      )
      rs.close()

      rs = stmtRec.executeQuery(s"show CREATE TABLE $tempTab")
      logDebug(s"=== show CREATE TABLE $tempTab")
      str.clear()
      while (rs.next()) {
        str ++= s"${rs.getString(1)}\t"
      }
      //todo need to find a better way to assert the result
      assert(str.toString().toUpperCase().contains("CREATE "))
      rs.close()

      rs = stmtRec.executeQuery("show tables in tapp")
      logDebug("\ntableNames in tapp:")
      str.clear()
      while (rs.next()) {
        val c2 = rs.getString("tableName")
        logDebug(c2)
        str ++= s"$c2\t"
      }
      assert(str.toString().toUpperCase().contains("TEST1COLTAB1")
          && str.toString().toUpperCase().contains("TEST1ROWTAB3"))
      rs.close()

      rs = stmtRec.executeQuery("show functions")

      logInfo("Functions :\n")
      str.clear()
      while (rs.next()) {
        str ++= s"${rs.getString("function")}\t"
      }
      assert(str.toString().toUpperCase().contains("GEMFIRE10.INTUDF1"))
      rs.close()

      rs = stmtRec.executeQuery(s"select *,intudf1(col2) as newcol from GEMFIRE10.test1coltab1")
      if (rs.next()) {
        assert(rs.getInt("newcol") === 6)
      }
      rs.close()

      rs = stmtRec.executeQuery("show schemas")
      logInfo("=== show schemas ===")
      str.clear()
      while (rs.next()) {
        str ++= s"${rs.getString("databaseName")}\t"
      }
      assert(str.toString().toUpperCase().contains("TAPP")
          && str.toString().toUpperCase().contains("GEMFIRE10"))
      rs.close()

      // custom diskstore test - column table
      rs = stmtRec.executeQuery("SELECT * FROM gemfire10.test1coltab4")
      resetBuffer
      arrBuf ++= ArrayBuffer("11,111", "333,33", "11,111", "333,33")
      while (rs.next()) {
        assert(s"${rs.getInt(1)},${rs.getInt(2)}" === arrBuf(i))
        i += 1
      }
      rs.close()

      //      describe table
      rs = stmtRec.executeQuery("describe gemfire10.test1coltab4")
      resetBuffer
      arrBuf ++= ArrayBuffer("COL1 - int", "COL2 - int")
      while (rs.next()) {
        assert(s"${rs.getString(1)} - ${rs.getString(2)}".equalsIgnoreCase(arrBuf(i)))
        i += 1
      }
      rs.close()

      // query view
      rs = stmtRec.executeQuery("select col1,* from gemfire10.vw_test1coltab1 ORDER BY 1")
      resetBuffer
      arrBuf ++= ArrayBuffer("1,1,aaaa,2.200000000000000000", "2,2,bbbb,3.300000000000000000")
      while (rs.next()) {
        assert(s"${rs.getInt(1)},${rs.getInt(2)},${rs.getString(3)},${rs.getBigDecimal(4)}"
            .equalsIgnoreCase(arrBuf(i)))
        i += 1
      }
      rs.close()


      // custom diskstore test - row row table
      rs = stmtRec.executeQuery("SELECT * FROM gemfire10.test1rowtab5 ORDER BY 1")
      arrBuf ++= ArrayBuffer("111,adsf", "111,adsf", "111,adsf", "2223,zxcvxcv", "2223,zxcvxcv",
        "2223,zxcvxcv")
      while (rs.next()) {
        assert(s"${rs.getInt(1)},${rs.getString(2)}" === arrBuf(i))
        i += 1
      }
      rs.close()

      rs = stmtRec.executeQuery("SELECT col1, col2, col3, col4, col5 from gemfire10" +
          ".test1rowtab6 ORDER BY col4")
      arrBuf.clear()
      i = 0
      arrBuf ++= ArrayBuffer("NULL,NULL,333.333,NULL,true", "NULL,xczadsf,232.1222,11,NULL",
        "NULL,adsf,NULL,12,false")
      while (rs.next()) {
        str.clear()
        str ++= s"${rs.getObject(1)},${rs.getString(2)},${rs.getObject(3)}" +
            s",${rs.getObject(4)},${rs.getObject(5)}"
        assert(str.toString().toUpperCase() === (arrBuf(i)).toUpperCase())
        i += 1
      }
      rs.close()

      rs = stmtRec.executeQuery("select col1, col2, col3, col4, col5 from" +
          " gemfire10.test1coltab7 ORDER BY col1")
      arrBuf.clear()
      i = 0
      arrBuf ++= ArrayBuffer("NULL,bb,345.123324,11,2019-03-21",
        "8123372036812312307,cc,NULL,NULL,NULL",
        "9123372036812312307,aa,123.123324,12,2019-03-20")
      while (rs.next()) {
        str.clear()
        str ++= s"${rs.getObject(1)},${rs.getObject(2)},${rs.getObject(3)}," +
            s"${rs.getObject(4)},${rs.getObject(5)}"
        assert(str.toString().toUpperCase() === (arrBuf(i)).toUpperCase())
        i += 1
      }
      rs.close()

      rs = stmtRec.executeQuery("SELECT * FROM gemfire10.test1coltab8 ORDER BY col1")
      arrBuf.clear()
      i = 0
      arrBuf ++= ArrayBuffer("null,null,null,null,null",
        "8123372036812312307,qewrwr4,345.123324,11,2019-03-21",
        "9123372036812312307,null,123.123324,12,null")

      while (rs.next()) {
        str.clear()
        str ++= s"${rs.getObject(1)},${rs.getObject(2)},${rs.getObject(3)}," +
            s"${rs.getObject(4)},${rs.getObject(5)}"
        assert(str.toString().toUpperCase() === (arrBuf(i)).toUpperCase())
        i += 1
      }
      rs.close()

      stmtRec.execute("call sys.EXPORT_DDLS('./recover_ddls_test1/');")

      stmtRec.close()
      connRec.close()
      afterEach()
      test_status = true

    } catch {
      case e: Throwable =>
        test_status = false
        throw new Exception(e)
    } finally {
      afterEach()
    }
  }


  //  test("test3 - All Data types at high volume") {
  def test3(): Unit = {
    try {
      // Focused particularly on checking if all data types can be
      // extracted properly
      // check for row and column type
      logDebug(s"Recovery_mode_dir: $recovery_mode_dir")
      confDirPath = createConfDir("test3")
      val leadsNum = 1
      val locatorsNum = 1
      val serversNum = 2
      workDirPath = createWorkDir("test3", leadsNum, locatorsNum, serversNum)

      val waitForInit = "-jobserver.waitForInitialization=true"
      val locatorPort = AvailablePortHelper.getRandomAvailableUDPPort

      val locNetPort = locatorNetPort
      val netPort2 = AvailablePortHelper.getRandomAvailableTCPPort
      val netPort3 = AvailablePortHelper.getRandomAvailableTCPPort
      val ldapConf = PrimaryDUnitRecoveryTest.getLdapConf

      logInfo(
        s"""PP: work dir path : $workDirPath
           | confdirpath: $confDirPath
           | ldapConf: $ldapConf""".stripMargin)

      // todo: test with 2 locators

      writeToFile(s"localhost  -peer-discovery-port=$locatorPort -recovery-state-chunk-size=20 -dir=$workDirPath/locator-1" +
          s" -client-port=$locNetPort $ldapConf", s"$confDirPath/locators")
      writeToFile(s"localhost  -locators=localhost[$locatorPort]  -dir=$workDirPath/lead-1" +
          s" $waitForInit $ldapConf", s"$confDirPath/leads")
      writeToFile(
        s"""localhost  -locators=localhost[$locatorPort] -recovery-state-chunk-size=50 -dir=$workDirPath/server-1 -client-port=$netPort2 $ldapConf
           |localhost  -locators=localhost[$locatorPort] -dir=$workDirPath/server-2 -client-port=$netPort3 $ldapConf
           |""".stripMargin, s"$confDirPath/servers")

      startSnappyCluster()

      var conn: Connection = null: Connection
      var stmt: Statement = null: Statement

      conn = getConn(locNetPort, "gemfire10", "gemfire10")
      stmt = conn.createStatement()

      stmt.execute(
        s"""CREATE TABLE gemfire10.test3tab1
            (col1 BIGINT NOT NULL, col2 INT NOT NULL, col3 INTEGER NOT NULL,
             col4 long NOT NULL, col5 short NOT NULL, col6 smallint NOT NULL,
             col7 byte NOT NULL, c1 tinyint NOT NULL, c2 varchar(22) NOT NULL,
             c3 string NOT NULL, c5 boolean NOT NULL,c6 double NOT NULL, c8 timestamp NOT NULL,
             c9 date NOT NULL, c10 decimal(15,5) NOT NULL, c11 numeric(20,10) NOT NULL,
             c12 float NOT NULL,c13 real not null, c14 binary, c15 blob, c16 clob, c17 char(1),
             LastThreeMatchPerformance ARRAY<Int>,Roll MAP<SMALLINT,STRING>,
             Profile STRUCT<Matches:Long,Runs:Int,SR:Double,isPlaying:Boolean>)
             USING COLUMN OPTIONS (BUCKETS '5', COLUMN_MAX_DELTA_ROWS '135');
                """)

      stmt.execute(
        s"""INSERT INTO gemfire10.test3tab1 select 9123372036854775807,2117483647,2116483647,
                  8223372036854775807,72,13,5,5,'qwerqwerqwer','qewrqewr',false,
                  912384020490234.91928374997239749824, '2019-02-18 15:31:55.333','2019-02-18',
                  2233.67234,4020490234.7239749824, 912384020490234.91928374997239749824,
                  920490234.9192837499724, cast ('aaa' as binary), cast('2234'
                  as blob), cast('adsf' as clob), 'b', ARRAY(15,7,12),MAP (4,'Bowler'),
                  STRUCT(46,123,106.95,true)
            """.stripMargin)

      stmt.execute(
        """INSERT INTO gemfire10.test3tab1 select id*100000000000000,id,id*100000000 ,
          |id*100000000000000,id,id, cast(id as byte), cast(id as tinyint), cast(concat('var',id)
          | as varchar(22)),  cast(concat('str',id) as string), cast(id%2 as Boolean),
          | cast((id*701*7699 + id*1342341*2267)/2 as double), CURRENT_TIMESTAMP, CURRENT_DATE,
          | cast(id*241/11 as Decimal(15,5)), cast(id*701/11 as Numeric(20,10)),
          | cast(concat(id*100,'.',(id+1)*7699) as float),
          | cast(concat(id*100000000000,'.',(id+1)*2267*7699) as real),
          | cast('binary_'|| id as binary), cast('blob_' || id as blob), cast(id*2 as clob),
          | 'a', ARRAY(15,7,12),
          | MAP(4,'Bowler'),STRUCT(46,123,106.95,true)
          | from range(500);
          | """.stripMargin)

      val rsTest3tab1 = stmt.executeQuery("SELECT * FROM gemfire10.test3tab1 ORDER BY col2")
      compareResultSet("gemfire10.test3tab1", rsTest3tab1, false)

      stmt.execute(
        s"""CREATE TABLE gemfire10.test3tab2
         (col1 BIGINT , col2 INT , col3 INTEGER , col4 long ,
         col5 short , col6 smallint , col7 byte , c1 tinyint ,
         c2 varchar(22) , c3 string , c5 boolean  ,c6 double ,
         c8 timestamp , c9 date , c10 decimal(15,5) , c11 numeric(20,10) ,
         c12 float ,c13 float, c14 char(3)
         , primary key (col2,col3))
         using row
         options (partition_by 'col1,col2,col3', buckets '5', COLUMN_MAX_DELTA_ROWS
         '135');
         """)

      stmt.execute(
        s"""INSERT INTO gemfire10.test3tab2 VALUES(9123372036854775807,2117483647,2116483647,
             8223372036854775807,72,13,5,5,'qwerqwerqwer','qewrqewr',false,
             912384020490234.91928374997239749824,
             '2019-02-18 15:31:55.333','2019-02-18',2233.67234,4020490234.7239749824,
             912384020490234.91928374997239749824,920490234.9192837499724, 'abc')"""
            .stripMargin)

      stmt.execute(
        s"""INSERT INTO gemfire10.test3tab2 select id*100000000000000,id,id*100000000,
           | id*100000000000000,id,id,
           | cast(id as byte),
           | cast(id as tinyint), cast(concat('var',id) as varchar(22)),  cast(concat('str',id)
           | as string),
           | cast(id%2 as Boolean), cast((id*701*7699 + id*1342341*2267)/2 as double),
           | CURRENT_TIMESTAMP,
           | CURRENT_DATE, cast(id*241/11 as Decimal(15,5)), cast(id*701/11 as Numeric(20,10)),
           | cast(concat(id*100,'.',(id+1)*7699) as float),
           | cast(concat(id*100000000000,'.',(id+1)*2267*7699) as real), 'asd' from range(5);
           |""".stripMargin)


      val rsTest3tab2 = stmt.executeQuery("SELECT * FROM gemfire10.test3tab2 ORDER BY col2")
      compareResultSet("gemfire10.test3tab2", rsTest3tab2, false)


      stmt.execute(
        s"""CREATE TABLE gemfire10.test3Reptab2
             (col1 BIGINT , col2 INT , col3 INTEGER not null, col4 long ,
             col5 short not null, col6 smallint , col7 byte not null, c1 tinyint ,
             c2 varchar(22) , c3 string , c5 boolean , c6 double ,
             c8 timestamp , c9 date not null, c10 decimal(15,5) ,
             c11 numeric(12,4) not null, c12 float ,c13 real not null, c14 binary,
             c15 blob, c16 char(4), primary key (col2)) using row
             options ();
                       """)
      stmt.execute(
        s"""INSERT INTO gemfire10.test3Reptab2
                  select id*100000000000000, id, id*100000000, id*100000000000000,
                  id, id, cast(id as byte), cast(id as tinyint), cast(concat('var',id) as varchar
                  (22)), cast(concat('str',id) as string), cast(id%2 as Boolean),
                  cast((id*701*7699 + id*1342341*2267)/2 as double), CURRENT_TIMESTAMP,
                  CURRENT_DATE, cast(id*241/11 as Decimal(15,5)), cast(id*701/11 as Numeric(12,4)),
                  cast(concat(id*100,'.',(id+1)*7699) as float), cast(concat(id*100000000000,'.',
                  (id+1)*2267*7699) as real), cast('aaaa' as binary), cast('yyyy' as blob),
                  'asdf' from range(5);
                 """.stripMargin)

      val rsTest3Reptab2 = stmt.executeQuery("SELECT * FROM gemfire10.test3Reptab2 ORDER BY col2")
      compareResultSet("gemfire10.test3Reptab2", rsTest3Reptab2, isRecoveredDataRS = false)

      // enable once support is added for primary key and binary,clob,blob
      // 3. Random mix n match data types
      stmt.execute("CREATE TABLE gemfire10.test3tab3 (col1 binary, col2 clob, col3 blob, col4 " +
          "varchar(44), col5 int, primary key (col5)) using row")
      stmt.execute("INSERT INTO gemfire10.test3tab3 select cast('a' as binary), cast('b' as clob)" +
          ", cast('1' as blob), 'adsf', 123")
      stmt.execute("INSERT INTO gemfire10.test3tab3 select cast('aa' as binary), cast('bb' as " +
          "clob), cast('11' as blob), 'adsfg', 1234")
      stmt.execute("INSERT INTO gemfire10.test3tab3 select cast('aaa' as binary), cast('bbb' as " +
          "clob), cast('1111' as blob), 'adsfgh', 12345")
      stmt.execute("INSERT INTO gemfire10.test3tab3 select cast('asdf' as binary), cast('bnm' as " +
          "clob), cast('1111111' as blob), 'adsfghi', 123456")

      // with option - key_columns
      stmt.execute("CREATE TABLE test3_coltab4 (col1 int, col2 string, col3 float) USING COLUMN" +
          " OPTIONS (key_columns 'col1')")
      stmt.execute("INSERT INTO test3_coltab4 VALUES(1,'aaa',123.122)")
      stmt.execute("INSERT INTO test3_coltab4 VALUES(2,'bbb',4444.55)")

      stmt.execute("CREATE TABLE test3rowtab5 (col1 FloaT NOT NULL, col2 TIMEstamp NOT NULL, col3" +
          " BOOLEAN NOT NULL," +
          " col4 varchar(1) NOT NULL, col5 integer NOT NULL) using row")
      stmt.execute("INSERT INTO test3rowtab5 VALUES(123.12321, '2019-02-18 15:31:55.333', 0, 'a'," +
          "12)")
      stmt.execute("INSERT INTO test3rowtab5 VALUES(222.12321, '2019-02-18 16:31:56.333', 0, 'b'," +
          "13)")
      stmt.execute("INSERT INTO test3rowtab5 VALUES(3333.12321, '2019-02-18 17:31:57.333', " +
          "'true', 'c',14)")

      stmt.execute("CREATE TABLE test3coltab6 (col1 BIGINT, col2 tinyint, col3 BOOLEAN)" +
          " using column")
      stmt.execute("INSERT INTO test3coltab6 VALUES(100000000000001, 5, true)")
      stmt.execute("INSERT INTO test3coltab6 VALUES(200000000000001, 4, true)")
      stmt.execute("INSERT INTO test3coltab6 VALUES(300000000000001, 3, false)")

      stmt.execute("CREATE TABLE test3coltab7 (col1 decimal(15,9) NOT NULL, col2 float NOT NULL, " +
          "col3 BIGint NOT NULL," +
          " col4 date NOT NULL, col5 string NOT NULL) using column options(BUCKETS '512')")
      stmt.execute("INSERT INTO test3coltab7 VALUES(891012.312321314, 1434124.123434134," +
          " 193471498234123, '2019-02-18', 'ZXcabcdefg')")
      stmt.execute("INSERT INTO test3coltab7 VALUES(91012.312321314, 34124.12343413," +
          " 243471498234123, '2019-04-18', 'qewrabcdefg')")
      stmt.execute("INSERT INTO test3coltab7 VALUES(1012.312321314, 4124.1234341," +
          " 333471498234123, '2019-03-18', 'adfcdefg')")


      // todo: Paresh: the peculiar case
      stmt.execute("CREATE TABLE test3rowtab8 (col1 string, col2 int, col3 varchar(33)," +
          " col4 boolean, col5 float)using row")
      stmt.execute("INSERT INTO test3rowtab8 VALUES('qewradfs',111, 'asdfqewr', true, 123.1234);")
      stmt.execute("INSERT INTO test3rowtab8 VALUES('adsffs',222, 'vzxcqewr', true, 4745.345345);")
      stmt.execute("INSERT INTO test3rowtab8 VALUES('xzcvadfs',444, 'zxcvzv', false, 78768.34);")

      stmt.execute(
        """CREATE TABLE gemfire10.test3rowtab9
            (col1 BIGINT , col2 INT , col3 INTEGER ,col4 long ,
            col5 short , col6 smallint , col7 byte , c1 tinyint ,
            c2 varchar(22) , c3 string , c5 boolean , c6 double ,
            c8 timestamp , c9 date , c10 decimal(15,5) ,
            c11 numeric(20,10) , c12 float ,c13 real ) USING ROW OPTIONS()""")

      stmt.execute(
        """INSERT INTO gemfire10.test3rowtab9 VALUES(null,null,null,
                   null,null,null,null,null,null,null,null,null,
                   null,null,null,null,
                   null,null)""")

      stmt.execute(
        """CREATE TABLE gemfire10.test3coltab10
                               (col1 BIGINT , col2 INT , col3 INTEGER ,col4 long ,
                                col5 short , col6 smallint , col7 byte , c1 tinyint ,
                                 c2 varchar(22) , c3 string , c5 boolean , c6 double ,
                                    c8 timestamp , c9 date , c10 decimal(15,5) ,
                                    c11 numeric(20,10) , c12 float ,c13 real )
                                    USING COLUMN OPTIONS(buckets '2',COLUMN_MAX_DELTA_ROWS '3')""")

      stmt.execute(
        """INSERT INTO gemfire10.test3coltab10 VALUES(null,null,null,
                   null,null,null,null,null,null,null,null,null,
                   null,null,null,null,
                   null,null);""")
      stmt.execute(
        """INSERT INTO gemfire10.test3coltab10 VALUES(null,null,null,
                   null,null,null,null,null,null,null,null,null,
                   null,null,null,null,
                   null,null);""")
      stmt.execute(
        """INSERT INTO gemfire10.test3coltab10 VALUES(null,null,null,
                   null,null,null,null,null,null,null,null,null,
                   null,null,null,null,
                   null,null);""")

      // wide schema
      stmt.execute(
        """CREATE TABLE IF NOT EXISTS gemfire10.test3coltab11 (SINGLE_ORDER_DID
          | BIGINT ,SYS_ORDER_ID VARCHAR(64) ,SYS_ORDER_VER INTEGER ,DATA_SNDG_SYS_NM VARCHAR(128)
          | ,SRC_SYS VARCHAR(20) ,SYS_PARENT_ORDER_ID VARCHAR(64) ,SYS_PARENT_ORDER_VER SMALLINT ,
          | PARENT_ORDER_TRD_DATE VARCHAR(20),PARENT_ORDER_SYS_NM VARCHAR(128) ,SYS_ALT_ORDER_ID
          | VARCHAR(64) ,TRD_DATE VARCHAR(20),GIVE_UP_BROKER VARCHAR(20) ,EVENT_RCV_TS TIMESTAMP ,
          | SYS_ROOT_ORDER_ID VARCHAR(64) ,GLB_ROOT_ORDER_ID VARCHAR(64) ,GLB_ROOT_ORDER_SYS_NM
          | VARCHAR(128) ,GLB_ROOT_ORDER_RCV_TS TIMESTAMP ,SYS_ORDER_STAT_CD VARCHAR(20) ,
          | SYS_ORDER_STAT_DESC_TXT VARCHAR(120) ,DW_STAT_CD VARCHAR(20) ,EVENT_TS TIMESTAMP,
          | ORDER_OWNER_FIRM_ID VARCHAR(20),RCVD_ORDER_ID VARCHAR(64) ,EVENT_INITIATOR_ID VARCHAR
          | (64),TRDR_SYS_LOGON_ID VARCHAR(64),SOLICITED_FG  VARCHAR(1),RCVD_FROM_FIRMID_CD
          | VARCHAR(20),RCV_DESK VARCHAR(20),SYS_ACCT_ID_SRC VARCHAR(64) ,CUST_ACCT_MNEMONIC
          | VARCHAR(128),CUST_SLANG VARCHAR(20) ,SYS_ACCT_TYPE VARCHAR(20) ,CUST_EXCH_ACCT_ID
          | VARCHAR(64) ,SYS_SECURITY_ALT_ID VARCHAR(64) ,TICKER_SYMBOL VARCHAR(32) ,
          | TICKER_SYMBOL_SUFFIX VARCHAR(20) ,PRODUCT_CAT_CD VARCHAR(20) ,SIDE VARCHAR(20) ,
          | LIMIT_PRICE DECIMAL(28, 8),STOP_PRICE DECIMAL(28, 8),ORDER_QTY DECIMAL(28, 4) ,
          | TOTAL_EXECUTED_QTY DECIMAL(28, 4) ,AVG_PRICE DECIMAL(28, 8) ,DAY_EXECUTED_QTY DECIMAL
          | (28, 4) ,DAY_AVG_PRICE DECIMAL(28, 8) ,REMNG_QTY DECIMAL(28, 4) ,CNCL_QTY DECIMAL(28,
          | 4) ,CNCL_BY_FG  VARCHAR(1) ,EXPIRE_TS TIMESTAMP ,EXEC_INSTR VARCHAR(64) ,TIME_IN_FORCE
          | VARCHAR(20),RULE80AF  VARCHAR(1) ,DEST_FIRMID_CD VARCHAR(20) ,SENT_TO_CONDUIT VARCHAR
          | (20) ,SENT_TO_MPID VARCHAR(20) ,RCV_METHOD_CD VARCHAR(20) ,LIMIT_ORDER_DISP_IND
          | VARCHAR(1) ,MERGED_ORDER_FG  VARCHAR(1) ,MERGED_TO_ORDER_ID VARCHAR(64),RCV_DEPT_ID
          | VARCHAR(20) ,ROUTE_METHOD_CD VARCHAR(20) ,LOCATE_ID VARCHAR(256) ,LOCATE_TS TIMESTAMP
          | ,LOCATE_OVERRIDE_REASON VARCHAR(2000) ,LOCATE_BROKER VARCHAR(256) ,
          | ORDER_BRCH_SEQ_TXT VARCHAR(20) ,IGNORE_CD VARCHAR(20),CLIENT_ORDER_REFID VARCHAR(64)
          | ,CLIENT_ORDER_ORIG_REFID VARCHAR(64) ,ORDER_TYPE_CD VARCHAR(20) ,SENT_TO_ORDER_ID
          | VARCHAR(64),ASK_PRICE DECIMAL(28, 8) ,ASK_QTY DECIMAL(28, 4) ,BID_PRICE DECIMAL(28,
          | 10) ,BID_QTY DECIMAL(28, 4) ,REG_NMS_EXCEP_CD VARCHAR(20),REG_NMS_EXCEP_TXT
          | VARCHAR(2000) ,REG_NMS_LINK_ID VARCHAR(64) ,REG_NMS_PRINTS  VARCHAR(1) ,
          | REG_NMS_STOP_TIME TIMESTAMP ,SENT_TS TIMESTAMP ,RULE92  VARCHAR(1) ,
          | RULE92_OVERRIDE_TXT VARCHAR(2000) ,RULE92_RATIO DECIMAL(25, 10) ,
          | EXMPT_STGY_BEGIN_TIME TIMESTAMP ,EXMPT_STGY_END_TIME TIMESTAMP ,
          | EXMPT_STGY_PRICE_INST VARCHAR(2000) ,EXMPT_STGY_QTY DECIMAL(28, 4) ,CAPACITY
          | VARCHAR(20) ,DISCRETION_QTY DECIMAL(28, 4) ,DISCRETION_PRICE VARCHAR(64) ,
          | BRCHID_CD VARCHAR(20) ,BASKET_ORDER_ID VARCHAR(64) ,PT_STRTGY_CD VARCHAR(20) ,
          | SETL_DATE VARCHAR(20),SETL_TYPE VARCHAR(20) ,SETL_CURR_CD VARCHAR(20) ,SETL_INSTRS
          |  VARCHAR(2000) ,COMMENT_TXT VARCHAR(2000) ,CHANNEL_NM VARCHAR(128) ,FLOW_CAT
          | VARCHAR(20) ,FLOW_CLASS VARCHAR(20) ,FLOW_TGT VARCHAR(20) ,ORDER_FLOW_ENTRY
          | VARCHAR(20) ,ORDER_FLOW_CHANNEL VARCHAR(20) ,ORDER_FLOW_DESK VARCHAR(20) ,
          | FLOW_SUB_CAT VARCHAR(20) ,STRTGY_CD VARCHAR(20) ,RCVD_FROM_VENDOR VARCHAR(20) ,
          | RCVD_FROM_CONDUIT VARCHAR(20) ,SLS_PERSON_ID VARCHAR(64) ,SYNTHETIC_FG  VARCHAR
          | (1) ,SYNTHETIC_TYPE VARCHAR(20) ,FXRT DECIMAL(25, 8) ,PARENT_CLREFID VARCHAR(64)
          | ,REF_TIME_ID INTEGER ,OPT_CONTRACT_QTY DECIMAL(28, 4) ,OCEAN_PRODUCT_ID BIGINT ,
          | CREATED_BY VARCHAR(64) ,CREATED_DATE TIMESTAMP ,FIRM_ACCT_ID BIGINT ,DEST VARCHAR
          | (20) ,CNTRY_CD VARCHAR(20) ,DW_SINGLE_ORDER_CAT VARCHAR(20) ,CLIENT_ACCT_ID
          | BIGINT ,EXTERNAL_TRDR_ID VARCHAR(64) ,ANONYMOUS_ORDER_FG  VARCHAR(1) ,
          | SYS_SECURITY_ALT_SRC VARCHAR(20) ,CURR_CD VARCHAR(20) ,EVENT_TYPE_CD VARCHAR(20)
          | ,SYS_CLIENT_ACCT_ID VARCHAR(64) ,SYS_FIRM_ACCT_ID VARCHAR(20) ,SYS_TRDR_ID
          | VARCHAR(64) ,DEST_ID INTEGER ,OPT_PUT_OR_CALL VARCHAR(20) ,SRC_FEED_REF_CD
          | VARCHAR(64) ,DIGEST_KEY VARCHAR(128) ,EFF_TS TIMESTAMP ,ENTRY_TS TIMESTAMP ,
          | OPT_STRIKE_PRICE DECIMAL(28, 8) ,OPT_MATURITY_DATE VARCHAR(20) ,ORDER_RESTR
          | VARCHAR(4) ,SHORT_SELL_EXEMPT_CD VARCHAR(4) ,QUOTE_TIME TIMESTAMP ,SLS_CREDIT
          | VARCHAR(20) ,SYS_SECURITY_ID VARCHAR(64) ,SYS_SECURITY_ID_SRC VARCHAR(20) ,
          | SYS_SRC_SYS_ID VARCHAR(20) ,SYS_ORDER_ID_UNIQUE_SUFFIX VARCHAR(20) ,DEST_ID_SRC
          | VARCHAR(4) ,GLB_ROOT_SRC_SYS_ID VARCHAR(20) ,GLB_ROOT_ORDER_ID_SUFFIX VARCHAR(64)
          | ,SYS_ROOT_ORDER_ID_SUFFIX VARCHAR(20) ,SYS_PARENT_ORDER_ID_SUFFIX VARCHAR(20) ,
          | CREDIT_BREACH_PERCENT DECIMAL(25, 10) ,CREDIT_BREACH_OVERRIDE VARCHAR(256) ,
          | INFO_BARRIER_ID VARCHAR(256) ,EXCH_PARTICIPANT_ID VARCHAR(64) ,REJECT_REASON_CD
          | VARCHAR(4) ,DIRECTED_DEST VARCHAR(20) ,REG_NMS_LINK_TYPE VARCHAR(20) ,
          | CONVER_RATIO DECIMAL(28, 9) ,STOCK_REF_PRICE DECIMAL(28, 8) ,CB_SWAP_ORDER_FG
          | VARCHAR(1) ,EV DECIMAL(28, 8) ,SYS_DATA_MODIFIED_TS TIMESTAMP ,CMSN_TYPE VARCHAR
          | (20) ,SYS_CREDIT_TRDR_ID VARCHAR(20) ,SYS_ENTRY_USER_ID VARCHAR(20) ,
          | OPEN_CLOSE_CD VARCHAR(20) ,AS_OF_TRD_FG  VARCHAR(1) ,HANDLING_INSTR VARCHAR(20)
          | ,SECURITY_DESC VARCHAR(512) ,MINIMUM_QTY DECIMAL(21, 6) ,CUST_OR_FIRM VARCHAR
          | (20) ,MAXIMUM_SHOW DECIMAL(21, 6) ,SECURITY_SUB_TYPE VARCHAR(20) ,
          | MULTILEG_RPT_TYPE VARCHAR(4) ,ORDER_ACTION_TYPE VARCHAR(4) ,BARRIER_STYLE
          | VARCHAR(4) ,AUTO_IOI_REF_TYPE VARCHAR(4) ,PEG_OFFSET_VAL DECIMAL(10, 2) ,
          | AUTO_IOI_OFFSET DECIMAL(28, 10) ,IOI_PRICE DECIMAL(28, 10) ,TGT_PRICE DECIMAL
          | (28, 10) ,IOI_QTY VARCHAR(64) ,IOI_ORDER_QTY DECIMAL(28, 4) ,CMSN VARCHAR(64) ,
          | SYS_LEG_REF_ID VARCHAR(64) ,TRADING_TYPE VARCHAR(4) ,EXCH_ORDER_ID VARCHAR(64) ,
          | DEAL_ID VARCHAR(64) ,ORDER_TRD_TYPE VARCHAR(4) ,CXL_REASON VARCHAR(64),name
          | String,LastThreeMatchPerformance ARRAY<Int>,Roll MAP<SMALLINT,STRING>,Profile
          | STRUCT<Matches:Long,Runs:Int,SR:Double,isPlaying:Boolean>) using column;"""
            .stripMargin)
      stmt.execute(
        """INSERT INTO gemfire10.test3coltab11 SELECT id ,'SYS_ORDER_ID',
          |id%10000,'DATA_SNDG_SYS_NM','name','name',id%100,'PARENT_ORDER_TRD_DT' ,
          |'PARENT_ORDER_SYS_NM','name','201601','GIVE_UP_BROKER',from_unixtime(unix_timestamp
          |('2018-01-01 01:00:00')+floor(rand()*31536000)),'SYS_ROOT_ORDER_ID',
          |'GLB_ROOT_ORDER_ID','GLB_ROOT_ORDER_SYS_NM',from_unixtime(unix_timestamp('2018-01-01
          |01:00:00')+floor(rand()*31536000)),'SYS_ORDER_STAT_CD','SYS_ORDER_STAT_DESC_TXT',
          |'name',from_unixtime(unix_timestamp('2018-01-01 01:00:00')+floor(rand()*31536000)),
          |'ORDER_OWNER_FIRM_ID' ,'RCVD_ORDER_ID','EVENT_INITIATOR_ID','TRDR_SYS_LOGON_ID','F',
          |'RCVD_FROM_FIRMID_CD','name','SYS_ACCT_ID_SRC','CUST_ACCT_MNEMONIC','CUST_SLANG',
          |'name','CUST_EXCH_ACCT_ID','SYS_SECURITY_ALT_ID','TICKER_SYMBOL',
          |'TICKER_SYMBOL_SUFFIX','name','name',abs(randn()*10000),abs(randn()*10000),abs(randn()
          |*10000),abs(randn()*10000),abs(randn()*10000),abs(randn()*10000),abs(randn()*10000),
          |abs(randn()*10000),abs(randn()*10000),'C' ,from_unixtime(unix_timestamp('2018-01-01
          |01:00:00')+floor(rand()*31536000)),'EXEC_INSTR','TIME_IN_FORCE','R','DEST_FIRMID_CD',
          |'SENT_TO_CONDUIT','SENT_TO_MPID','RCV_METHOD_CD','I','M','MERGED_TO_ORDER_ID',
          |'RCV_DEPT_ID','ROUTE_METHOD_CD','LOCATE_ID',from_unixtime(unix_timestamp('2018-01-01
          |01:00:00')+floor(rand()*31536000)),lpad('LOCATE_OVERRIDE_REASON',1000,'abcdefg'),
          |'LOCATE_BROKER','ORDER_BRCH_SEQ_TXT','IGNORE_CD','CLIENT_ORDER_REFID',
          |'CLIENT_ORDER_ORIG_REFID','ORDER_TYPE_CD','SENT_TO_ORDER_ID',abs(randn()*10000),abs
          |(randn()*10000),abs(randn()*10000),abs(randn()*10000),'REG_NMS_EXCEP_CD',lpad
          |('REG_NMS_EXCEP_TXT',1000,'lmopqrst'),'REG_NMS_LINK_ID','P',from_unixtime
          |(unix_timestamp('2018-01-01 01:00:00')+floor(rand()*31536000)),from_unixtime
          |(unix_timestamp('2018-01-01 01:00:00')+floor(rand()*31536000)),'R',lpad
          |('RULE92_OVERRIDE_TXT',1000,'ijklmnop'),abs(randn()*10000),from_unixtime
          |(unix_timestamp('2018-01-01 01:00:00')+floor(rand()*31536000)),from_unixtime
          |(unix_timestamp('2018-01-01 01:00:00')+floor(rand()*31536000)),lpad
          |('EXMPT_STGY_PRICE_INST',500,'ijklmnop'),abs(randn()*10000),'CAPACITY' ,abs(randn()
          |*10000),'DISCRETION_PRICE','BRCHID_CD','BASKET_ORDER_ID','PT_STRTGY_CD','SETL_DATE',
          |'SETL_TYPE','SETL_CURR_CD',lpad('SETL_INSTRS',500,'ijklmnop'),lpad('COMMENT_TXT',500,
          |'ijklmnop'),'CHANNEL_NM','FLOW_CAT','FLOW_CLASS','FLOW_TGT','ORDER_FLOW_ENTRY',
          |'ORDER_FLOW_CHANNEL','ORDER_FLOW_DESK','FLOW_SUB_CAT','STRTGY_CD','RCVD_FROM_VENDOR',
          |'RCVD_FROM_CONDUIT','SLS_PERSON_ID','G','SYNTHETIC_TYPE',abs(randn()*10000),
          |'PARENT_CLREFID' ,id%1000,abs(randn()*10000),id,'CREATED_BY',from_unixtime
          |(unix_timestamp('2018-01-01 01:00:00')+floor(rand()*31536000)),id,'DEST','CNTRY_CD',
          |'DW_SINGLE_ORDER_CAT',id,'EXTERNAL_TRDR_ID','A','SYS_SECURITY_ALT_SRC','CURR_CD',
          |'EVENT_TYPE_CD','SYS_CLIENT_ACCT_ID','SYS_FIRM_ACCT_ID','SYS_TRDR_ID',id%10000,
          |'OPT_PUT_OR_CALL','SRC_FEED_REF_CD','DIGEST_KEY',from_unixtime(unix_timestamp
          |('2018-01-01 01:00:00')+floor(rand()*31536000)),from_unixtime(unix_timestamp
          |('2018-01-01 01:00:00')+floor(rand()*31536000)),abs(randn()*10000),
          |'OPT_MATURITY_DATE','ABCD','WXYZ',from_unixtime(unix_timestamp('2018-01-01 01:00:00')
          |+floor(rand()*31536000)),'SLS_CREDIT','SYS_SECURITY_ID','SYS_SECURITY_ID_SRC',
          |'SYS_SRC_SYS_ID','SYS_ORDER_ID_UNIQUE','DEST','GLB_ROOT_SRC_SYS_ID',
          |'GLB_ROOT_ORDER_ID','SYS_ROOT_ORD','SYS_PARENT_ORDER_ID',abs(randn()*10000),
          |'CREDIT_BREACH_OVERRIDE','INFO_BARRIER_ID','EXCH_PARTICIPANT_ID','REJT',
          |'DIRECTED_DEST','REG_NMS_LINK_TYPE',abs(randn()*10000),abs(randn()*10000),'C',abs
          |(randn()*10000),from_unixtime(unix_timestamp('2018-01-01 01:00:00')+floor(rand()
          |*31536000)),'CMSN_TYPE','SYS_CREDIT_TRDR_ID','SYS_ENTRY_USER_ID','OPEN_CLOSE_CD','S',
          |'HANDLING_INSTR','SECURITY_DESC',abs(randn()*10000),'CUST_OR_FIRM',abs(randn()*10000),
          |'SECURITY_SUB_TYPE','MULT','ORDR','BARE','AUTO',abs(randn()*10000),abs(randn()*10000),
          |abs(randn()*10000),abs(randn()*10000),'IOI_QTY',abs(randn()*10000),'CMSN',
          |'SYS_LEG_REF_ID','TRAD','EXCH_ORDER_ID','DEAL_ID','ORDR','CXL_REASON','Ravichandran
          |Ashwin',ARRAY(15,7,12),MAP(4,'Bowler'),STRUCT(46,123,106.95,true) from range(3);"""
            .stripMargin)
      val querytest3coltab11 = "SELECT * FROM gemfire10.test3coltab11 ORDER BY 1"
      val rstest3coltab11 = stmt.executeQuery(querytest3coltab11)

      compareResultSet("gemfire10.test3coltab11", rstest3coltab11, isRecoveredDataRS = false)

      // todo: alter table -add/drop column-

      stmt.close()
      conn.close()

      stopCluster()
      startSnappyRecoveryCluster()
      Thread.sleep(5000)

      var connRec: Connection = null: Connection
      var stmtRec: Statement = null: Statement
      var str = new mutable.StringBuilder()
      val arrBuf: ArrayBuffer[String] = ArrayBuffer.empty
      var i = 0

      def resetBuffer = {
        arrBuf.clear()
        i = 0
      }

      connRec = getConn(locNetPort, "gemfire10", "gemfire10")
      stmtRec = connRec.createStatement()

      var rs = stmtRec.executeQuery("SELECT * FROM gemfire10.test3tab1 ORDER BY col2")
      compareResultSet("gemfire10.test3tab1", rs, true)
      rs.close()

      rs = stmtRec.executeQuery("SELECT * FROM gemfire10.test3tab2 ORDER BY col2")
      compareResultSet("gemfire10.test3tab2", rs, true)
      rs.close()

      rs = stmtRec.executeQuery("select col1, col2, col3, col4," +
          " col5 from gemfire10.test3tab3 ORDER BY col5")
      println("select * from test3tab3 =======================")
      resetBuffer

      arrBuf ++= ArrayBuffer("a,b,1,adsf,123", "aa,bb,11,adsfg,1234", "aaa,bbb,1111,adsfgh," +
          "12345", "asdf,bnm,1111111,adsfghi,123456")
      while (rs.next()) {
        // scalastyle:off println
        println(s"${rs.getBlob(1)},${rs.getClob(2)},${rs.getBlob(3)}," +
            s"${rs.getString(4)},${rs.getInt(5)}")
        assert(s"${scala.io.Source.fromInputStream(rs.getBlob(1).getBinaryStream).mkString}," +
            s"${scala.io.Source.fromInputStream(rs.getClob(2).getAsciiStream).mkString}," +
            s"${scala.io.Source.fromInputStream(rs.getBlob(3).getBinaryStream).mkString}," +
            s"${rs.getString(4)},${rs.getInt(5)}" == arrBuf(i))
        i += 1
      }

      rs = stmtRec.executeQuery("select col1, col2, col3 from" +
          " gemfire10.test3_coltab4 ORDER BY col1")
      resetBuffer
      arrBuf ++= ArrayBuffer("1,aaa,123.122", "2,bbb,4444.55")
      while (rs.next()) {
        assert(s"${rs.getInt("col1")},${rs.getString("col2")},${rs.getFloat("col3")}"
            .equalsIgnoreCase(arrBuf(i)))
        i += 1
      }
      assert(i != 0)
      rs.close()

      rs = stmtRec.executeQuery("SELECT col1, col2, col3 from gemfire10.test3coltab6 ORDER BY col1")
      arrBuf.clear()
      i = 0
      arrBuf ++= ArrayBuffer("100000000000001,5,true", "200000000000001,4,true",
        "300000000000001,3,false")
      while (rs.next()) {
        assert(s"${rs.getLong("col1")},${rs.getShort("col2")},${rs.getBoolean("col3")}"
            .equalsIgnoreCase(arrBuf(i)),
          s"Got: ${rs.getLong("col1")}, ${rs.getShort("col2")}," +
              s" ${rs.getBoolean("col3")}\nExpected: ${arrBuf(i)}")
        i += 1
      }
      assert(i != 0)
      rs.close()


      rs = stmtRec.executeQuery("select count(*) rcount, c5 from gemfire10.test3tab1" +
          " group by c5 having c5 = true ORDER BY c5;")
      str.clear()
      while (rs.next()) {
        str ++= s"${rs.getInt("rcount")},${rs.getBoolean("c5")}"
      }
      assert(str.toString().equalsIgnoreCase("250,true"), s"Actual: ${str.toString()}")
      rs.close()

      // 4. Test if all sql functions are working fine - like min,max,avg,etc.
      //    Test if individual columns can be queried

      rs = stmtRec.executeQuery("select first(col3) as fCol3, max(col1) as maxCol1," +
          " round(avg(col1)) as avgRoundRes, count(*) as count,concat('str_',first(col4)) as" +
          " concatRes, cast(first(col1) as string) as castRes, isnull(max(col5)) as isNullRes," +
          " Current_Timestamp, day(current_timestamp) from gemfire10.test3rowtab5;")
      assert(rs.next() === true)
      assert(rs.getFloat("maxcol1") === 3333.1233F && rs.getInt("count") === 3)

      rs.close()

      rs = stmtRec.executeQuery("SELECT * FROM gemfire10.test3Reptab2 ORDER BY col2;")
      compareResultSet("gemfire10.test3Reptab2", rs, true)
      rs.close()

      rs = stmtRec.executeQuery("SELECT * FROM gemfire10.test3coltab7 ORDER BY col3;")
      resetBuffer

      arrBuf ++= ArrayBuffer("891012.312321314,1434124.125,193471498234123,2019-02-18,ZXcabcdefg",
        "91012.312321314,34124.125,243471498234123,2019-04-18,qewrabcdefg",
        "1012.312321314,4124.12353515625,333471498234123,2019-03-18,adfcdefg")
      while (rs.next()) {
        assert(s"${rs.getBigDecimal(1)},${rs.getDouble(2)}" +
            s",${rs.getLong(3)},${rs.getDate(4)},${rs.getString(5)}" === arrBuf(i))
        i += 1
      }
      assert(i != 0)
      rs.close()

      rs = stmtRec.executeQuery("SELECT * FROM gemfire10.test3rowtab8 ORDER BY col2;")
      resetBuffer
      arrBuf ++= ArrayBuffer("qewradfs,111,asdfqewr,true,123.1234",
        "adsffs,222,vzxcqewr,true,4745.345", "xzcvadfs,444,zxcvzv,false,78768.34")
      while (rs.next()) {
        assert(s"${rs.getString(1)},${rs.getInt(2)},${rs.getString(3)}," +
            s"${rs.getBoolean(4)},${rs.getFloat(5)}" === arrBuf(i))
        i += 1
      }
      assert(i != 0)
      rs.close()

      rs = stmtRec.executeQuery("SELECT * FROM gemfire10.test3rowtab9 ORDER BY col2;")
      resetBuffer
      arrBuf ++= ArrayBuffer("null,null,null,null,null,null,null,null,null,null,null,null,null," +
          "null,null,null,null,null")

      while (rs.next()) {
        assert(s"${rs.getObject(1)},${rs.getObject(2)},${rs.getObject(3)},${rs.getObject(4)}," +
            s"${rs.getObject(5)},${rs.getObject(6)},${rs.getObject(7)},${rs.getObject(8)}," +
            s"${rs.getObject(9)},${rs.getObject(10)},${rs.getObject(11)}," +
            s"${rs.getObject(12)},${rs.getObject(13)},${rs.getObject(14)}," +
            s"${rs.getObject(15)},${rs.getObject(16)},${rs.getObject(17)}," +
            s"${rs.getObject(18)}" === arrBuf(i))
        i += 1
      }
      assert(i != 0)
      rs.close()

      rs = stmtRec.executeQuery("SELECT * FROM gemfire10.test3coltab10 ORDER BY col2;")
      resetBuffer
      arrBuf ++= ArrayBuffer("null,null,null,null,null,null,null,null,null,null,null,null,null" +
          ",null,null,null,null,null", "null,null,null,null,null,null,null,null,null,null,null," +
          "null,null,null,null,null,null,null", "null,null,null,null,null,null,null,null,null," +
          "null,null,null,null,null,null,null,null,null")

      while (rs.next()) {
        assert(s"${rs.getObject(1)},${rs.getObject(2)},${rs.getObject(3)},${rs.getObject(4)}," +
            s"${rs.getObject(5)},${rs.getObject(6)},${rs.getObject(7)},${rs.getObject(8)}," +
            s"${rs.getObject(9)},${rs.getObject(10)},${rs.getObject(11)}," +
            s"${rs.getObject(12)},${rs.getObject(13)},${rs.getObject(14)}," +
            s"${rs.getObject(15)},${rs.getObject(16)},${rs.getObject(17)}," +
            s"${rs.getObject(18)}" === arrBuf(i))
        i += 1
      }
      assert(i != 0)
      rs.close()

      rs = stmtRec.executeQuery(querytest3coltab11)
      compareResultSet("gemfire10.test3coltab11", rs, isRecoveredDataRS = true)

      stmtRec.execute("call sys.EXPORT_DDLS('./recover_ddls_test3/');")
      stmtRec.close()
      conn.close()
      afterEach()
      test_status = true
    } catch {
      case e: Throwable =>
        test_status = false
        throw new Exception(e)
    } finally {
      afterEach()
    }
  }

  //  //  test("test2 - Does all basic tests in non-secure mode(without LDAP).") {
  //  def test2(): Unit = {
  //    // although ldap server is started before all, if ldap properties are not passed to conf,
  //    // it should work in non-secure mode.
  //    // basicOperationSetSnappyCluster can be used
  //    // multiple VMs - multiple servers - like real world scenario
  //
  //
  //    // check for row and column type
  //
  //
  //    // After the cluster has come up and ready to be used by user.
  //    // check if all procedures available to user is working fine
  //    afterEach()
  //    test_status = true
  //  }

  //  test("test4 - When partial cluster is not available/corrupted/deleted") {
  //  def test4(): Unit = {
  //    // check for row and column type
  //
  //    // 1. what if one of diskstores is deleted - not available.
  //    // 2. what if some .crf files are missing
  //    // 3. what if some .drf files are missing
  //    // 4. what if some .krf files are missing
  //    afterEach()
  //    test_status = true
  //  }

  //  test("test5 -Recovery procedures / Data export performance check") {
  def test5(): Unit = {
    try {
      // todo: Should be able to recover data and export to S3, hdfs, nfs and local file systems.
      // todo: Recover data in parquet format, test that, reloading the table from the same
      // parquet file should work fine.

      confDirPath = createConfDir("test5")
      val leadsNum = 1
      val locatorsNum = 1
      val serversNum = 1
      workDirPath = createWorkDir("test5", leadsNum, locatorsNum, serversNum)
      val waitForInit = "-jobserver.waitForInitialization=true"
      val locatorPort = AvailablePortHelper.getRandomAvailableUDPPort
      val locNetPort = locatorNetPort
      val netPort2 = AvailablePortHelper.getRandomAvailableTCPPort
      val netPort3 = AvailablePortHelper.getRandomAvailableTCPPort
      val ldapConf = PrimaryDUnitRecoveryTest.getLdapConf

      writeToFile(s"localhost  -peer-discovery-port=$locatorPort -dir=$workDirPath/locator-1" +
          s" -client-port=$locNetPort $ldapConf", s"$confDirPath/locators")
      writeToFile(s"localhost  -locators=localhost[$locatorPort]  -dir=$workDirPath/lead-1" +
          s" $waitForInit $ldapConf", s"$confDirPath/leads")
      writeToFile(
        s"""localhost  -locators=localhost[$locatorPort] -recovery-state-chunk-size=20 -dir=$workDirPath/server-1 -client-port=$netPort2 $ldapConf
           |""".stripMargin, s"$confDirPath/servers")

      startSnappyCluster()
      var conn: Connection = null: Connection
      var stmt: Statement = null: Statement
      conn = getConn(locNetPort, "gemfire10", "gemfire10")
      stmt = conn.createStatement()
      val rowNum = 500

      stmt.execute("CREATE TABLE test5coltab1 (col1 float, col2 int, col3 string,col4 date," +
          " col5 tinyint) USING COLUMN OPTIONS(buckets '1')")
      stmt.execute("INSERT INTO test5coltab1 select id*433/37, id, concat('str_',id)," +
          s" '2019-03-18', id%5 from range($rowNum);")

      stmt.execute("CREATE TABLE test5coltab2 (col1 float, col2 bigint, col3 varchar(99)," +
          " col4 date, col5 byte, col6 short) USING COLUMN OPTIONS(buckets '6')")
      stmt.execute("INSERT INTO test5coltab2 select cast(id*433/37 as float),id*999999," +
          " concat('str_',id), '2019-03-18', cast(id as tinyint)," +
          s" id%32 from range(${rowNum * 100})")

      stmt.execute("CREATE TABLE test5rowtab3 (col1 float , col2 int, col3 string ,col4 date," +
          " col5 tinyint) USING ROW OPTIONS(partition_by 'col2,col5', buckets '1')")

      stmt.execute("ALTER TABLE test5rowtab3 ADD CONSTRAINT CONS_UNIQUE_1 UNIQUE (col2) ")

      stmt.execute("INSERT INTO test5rowtab3 select id*433/37, id, concat('str_',id)," +
          s" '2019-03-18', id%5 from range($rowNum);")

      stmt.execute("CREATE TABLE gemfire10.test5rowtab4 (col1 float, col2 bigint ," +
          " col3 varchar(99), col4 date , col5 byte, col6 short) using row" +
          " options(partition_by 'col2,col6', buckets '6')")
      stmt.execute(s"INSERT INTO gemfire10.test5rowtab4 select cast(id*433/37 as float)," +
          s"id*999999, concat('str_',id),'2019-03-18',cast(id as tinyint)," +
          s" id%32 from range(${rowNum * 100})")

      writeToFile("1,aaaa,11.11\n2,bbbb,222.2\n333,ccccc,333.33", "/tmp/test5_exttab1.csv")
      stmt.execute("create external table test5_exttab1 using csv" +
          " options(path '/tmp/test5_exttab1.csv')")

      // case: CREATE TABLE as SELECT * FROM ...
      stmt.execute("CREATE TABLE test5coltab4 as SELECT * FROM test5_exttab1")


      stmt.execute("ALTER TABLE test5rowtab4 add constraint cons_check_1 check(col6 >= 0)")

      // column table - how nulls are reflected in the recovered data files.
      stmt.execute("CREATE TABLE test5coltab5 (col1 timestamp, col2 integer, col3 varchar(33)," +
          "col boolean) using column")
      stmt.execute("INSERT INTO test5coltab5 values(null, 123, 'adsfqwer', 'true')")
      stmt.execute("INSERT INTO test5coltab5 values(null, null, 'zxcvqwer', null)")
      stmt.execute("INSERT INTO test5coltab5 values(null, 12345, 'ZXcwer', 'true')")
      stmt.execute("INSERT INTO test5coltab5 values(null, 67653, null, null)")

      // row table - how nulls reflect in the recovered data files.
      // todo: fix this:default fails in createSchemasMap method of PrimaryDUnitRecoveryTest
      stmt.execute("CREATE TABLE test5_rowtab6 (col1 int, col2 string default 'DEF_VAL'," +
          " col3  long default -99999, col4 float default 0.0)")
      stmt.execute("INSERT INTO test5_rowtab6 values(null, 'afadsf', 134098245, 123.123)")
      stmt.execute("INSERT INTO test5_rowtab6 values(null, 'afadsf', 134098245, 123.123)")
      stmt.execute("INSERT INTO test5_rowtab6 values(null, null, null, null)")
      stmt.execute("INSERT INTO test5_rowtab6 (col1,col3) values(null, 134098245 )")
      stmt.execute("INSERT INTO test5_rowtab6 values(null, 'afadsf', 134098245 )")
      stmt.execute("INSERT INTO test5_rowtab6 (col1, col4) values(null, 345345.534)")

      stmt.execute("CREATE TABLE test5coltab7 (c3 Array<Varchar(400)>, c4 Map < Int, Double > NOT" +
          " NULL) using column")

      stmt.execute("deploy package SPARKREDSHIFT" +
          " 'com.databricks:spark-redshift_2.10:3.0.0-preview1' path '/tmp/deploy_pkg_cache'")
      stmt.execute("deploy package Sparkcassandra 'com.datastax" +
          ".spark:spark-cassandra-connector_2.11:2.0.7';")
      stmt.execute("deploy package MSSQL 'com.microsoft.sqlserver:sqljdbc4:4.0'" +
          " repos 'http://clojars.org/repo/'")
      stmt.execute("deploy package mysql 'clj-mysql:clj-mysql:0.1.0'" +
          " repos 'http://clojars.org/repo/' path '/tmp/deploy_pkg_cache'")
      stmt.execute(s"deploy jar snappyjar" +
          s" '${PrimaryDUnitRecoveryTest.snappyHome}/jars/zkclient-0.8.jar'")
      stmt.execute(s"deploy jar snappyjar2" +
          s" '${PrimaryDUnitRecoveryTest.snappyHome}/jars/zookeeper-3.4.13.jar'")
      stmt.execute("undeploy snappyjar")
      stmt.execute("undeploy Sparkcassandra")

      stmt.execute("call sys.EXPORT_DDLS('./regularmode_ddls_test5');")

      stmt.close()
      conn.close()

      stopCluster()
      startSnappyRecoveryCluster()

      Thread.sleep(2500)

      var connRec: Connection = null
      var stmtRec: Statement = null

      logInfo("=== Recovery mode ============\n")
      connRec = getConn(locNetPort, "gemfire10", "gemfire10")
      stmtRec = connRec.createStatement()
      // todo: may be we can add S3,hdfs as export path

      stmtRec.execute("call sys.EXPORT_DATA('./recover_data_parquet','parquet'," +
          "'  gemfire10.test5coltab1   ','true')")

      logInfo(s"EXPORT_DATA called for test5coltab2 at ${System.currentTimeMillis}")
      stmtRec.execute("call sys.EXPORT_DATA('./recover_data_parquet','parquet'," +
          "'   gemfire10.test5coltab2','true')")
      logInfo(s"EXPORT_DATA ends for test5coltab2 at ${System.currentTimeMillis}")


      logInfo(s"EXPORT_DATA called for test5rowtab4 at ${System.currentTimeMillis}")
      stmtRec.execute("call sys.EXPORT_DATA('./recover_data_parquet','parquet'," +
          "'gemfire10.test5rowtab4','true')")
      logInfo(s"EXPORT_DATA ends for test5rowtab4 at ${System.currentTimeMillis}")

      // checks ignore_error
      stmtRec.execute("call sys.EXPORT_DATA('./recover_data_parquet'," +
          "'parquet','gemfire10.test5coltab2,gemfire10.test5rowtab4, NonExistentTable','true')")

      stmtRec.execute("call sys.EXPORT_DATA('./recover_data_json','json'," +
          "'gemfire10.test5rowtab3','true')")

      stmtRec.execute("call sys.EXPORT_DATA('./recover_data_all','csv','all','true')")
      // todo how to verify if the files are correct?

      // check DLLs - create table, diskstore, view, schema, external table, index,
      // alter table -add/drop column-,

      // - drop diskstore, index, table, external table, schema, rename
      // create function

      stmtRec.execute("call sys.EXPORT_DDLS('./recover_ddls_test5');")
      new File("/tmp/test5_exttab1.csv").delete()
      // todo: add assertion for recover_ddls

      //      import reflect.io._, Path._
      //      "find -name recover_ddls_test5*".!!.trim.toDirectory.files.foreach(println)
      //      "find -name regularmode_ddls_test5*".!!.trim.toDirectory.files.foreach(println)
      //
      //      val recoverymodeDdlFile = {for (elem <- "find -name recover_ddls_test5*".!!.trim
      // .toDirectory.files; if(elem.name == "part-00000")) yield elem}.next()
      //      val regularmodeDdlFile = {for (elem <- "find -name regularmode_ddls_test5*".!!.trim
      // .toDirectory.files; if(elem.name == "part-00000")) yield elem }.next()
      //      val cmd = s"comm --nocheck-order -3 ${recoverymodeDdlFile.path}
      // ${recoverymodeDdlFile.path}"

      stmtRec.close()
      connRec.close()

      afterEach()
      test_status = true

    } catch {
      case e: Throwable =>
        test_status = false
        throw new Exception(e)
    } finally {
      afterEach()
    }
  }


  //  test("test6 - update, delete, complex data types") {
  def test6(): Unit = {
    // Add test cases that has to fail
    // Add test cases for sample tables
    // Add test for S3, hdfs
    // Add binary and blob in the tests
    // Add not nulls to the tests
    // Add assertion fo recover_ddl / recover_data output
    try {
      confDirPath = createConfDir("test6")
      val leadsNum = 1
      val locatorsNum = 1
      val serversNum = 1
      workDirPath = createWorkDir("test6", leadsNum, locatorsNum, serversNum)
      val waitForInit = "-jobserver.waitForInitialization=true"
      val locatorPort = AvailablePortHelper.getRandomAvailableUDPPort
      val locNetPort = locatorNetPort
      val netPort2 = AvailablePortHelper.getRandomAvailableTCPPort
      val netPort3 = AvailablePortHelper.getRandomAvailableTCPPort
      val ldapConf = PrimaryDUnitRecoveryTest.getLdapConf
      writeToFile(s"localhost  -peer-discovery-port=$locatorPort -dir=$workDirPath/locator-1" +
          s" -client-port=$locNetPort $ldapConf", s"$confDirPath/locators")
      writeToFile(s"localhost  -locators=localhost[$locatorPort]  -dir=$workDirPath/lead-1" +
          s" $waitForInit $ldapConf", s"$confDirPath/leads")
      writeToFile(
        s"localhost  -locators=localhost[$locatorPort] -recovery-state-chunk-size=40 -dir=$workDirPath/server-1 " +
            s"-client-port=$netPort2 $ldapConf".stripMargin, s"$confDirPath/servers")

      startSnappyCluster()

      val conn = getConn(locNetPort, "gemfire10", "gemfire10")
      val stmt = conn.createStatement()
      val defaultSchema = "gemfire10"
      var fqtn: String = null

      // todo: Add nested complex data types tests
      // todo: null values not supported for complex data type columns - check

      // ========================================
      // ==== Column tables column batch only ===
      // ========================================
      // 1: null and not null, atomic data only, 1 bucket
      fqtn = "gemfire10.t1"
      stmt.executeUpdate(
        s"""CREATE TABLE $fqtn (c1 integer, c2 double NOT NULL,
           |c3 integer) USING COLUMN options (buckets '1', COLUMN_MAX_DELTA_ROWS '4')"""
            .stripMargin)
      stmt.execute(s"INSERT INTO $fqtn VALUES (1, 111111111111111111.11111111111111111111, 11)")
      stmt.execute(s"INSERT INTO $fqtn VALUES (2, 222222222222222222.22222222222222222222, null)")
      stmt.execute(s"INSERT INTO $fqtn VALUES (3, 322222222222222222.22222222222222222222, 33)")
      stmt.execute(s"INSERT INTO $fqtn VALUES (4, 422222222222222222.22222222222222222222, null)")

      // 2: null and not null complex types 1 bucket
      fqtn = "gemfire10.t2"
      stmt.executeUpdate(
        s"""CREATE TABLE $fqtn (c1 integer, c2 double,c3 Array<date>,
           |c4 Map<Int,date> NOT NULL, c5 Struct<f1:float, f2:date>, c6 Array<timestamp>)
           | USING COLUMN options (buckets '1', COLUMN_MAX_DELTA_ROWS '3')""".stripMargin)
      stmt.executeUpdate(s"INSERT INTO $fqtn SELECT" +
          s" 1, 1.1, Array('2011-11-11', '2010-12-12', null), Map(1,'2011-11-11')," +
          s" Struct(1.1, '2011-11-11'), Array(null, " +
          s"'2022-02-22 22:22:22.222')")
      stmt.executeUpdate(s"INSERT INTO $fqtn SELECT" +
          s" 2, null, Array(null,'2022-11-11', '2044-06-06', '2006-06-05')," +
          s" Map(2, cast(null as date)), Struct(2.2, cast(null as date))," +
          s" Array('2019-02-18 15:31:55.333', '2022-02-22 22:22:22.222')")
      stmt.executeUpdate(s"INSERT INTO $fqtn SELECT" +
          s" 3, 3.3, Array('2044-11-11', '2088-12-12'), Map(3,'2044-11-11'), " +
          s"Struct(3.3, '2044-11-11'), Array('2019-02-18 15:31:55.333', null)")

      // ==========================================
      // ====== Column tables row buffer only =====
      // ==========================================
      // 3: null and not null atomic data only, 2 buckets
      fqtn = "gemfire10.t3"
      stmt.executeUpdate(
        s"""CREATE TABLE $fqtn (c1 integer, c2 double NOT NULL,
           |c3 integer) USING COLUMN options (buckets '2', COLUMN_MAX_DELTA_ROWS '5')"""
            .stripMargin)
      stmt.execute(s"INSERT INTO $fqtn VALUES (1, 111111111111111111.11111111111111111111, null)")
      stmt.execute(s"INSERT INTO $fqtn VALUES (2, 222222222222222222.22222222222222222222, 22)")
      stmt.execute(s"INSERT INTO $fqtn VALUES (3, 322222222222222222.22222222222222222222, 33)")


      // 4: null and not null complex types 2 buckets
      fqtn = "gemfire10.t4"
      stmt.executeUpdate(
        s"""CREATE TABLE $fqtn (c1 integer, c2 double,c3 Array<date>,
           |c4 Map<Int,date> NOT NULL, c5 Struct<f1:float, f2:date>, c6 Array<timestamp>)
           | USING COLUMN options (buckets '2', COLUMN_MAX_DELTA_ROWS '4')""".stripMargin)
      stmt.executeUpdate(s"INSERT INTO $fqtn SELECT" +
          s" 1, 1.1, Array(null,'2022-11-11', '2044-06-06', '2006-06-05'), Map(1,cast(null as " +
          s"date)), Struct(1.1, cast(null as date)), Array(null, '2022-02-22 22:22:22.222')")
      stmt.executeUpdate(s"INSERT INTO $fqtn SELECT" +
          s" 2, null, Array('2011-11-11', '2010-12-12', null), Map(2,'2011-11-11')," +
          s" Struct(2.2, '2011-11-11'), Array('2019-02-18 15:31:55.333', '2022-02-22 " +
          s"22:22:22.222')")
      stmt.executeUpdate(s"INSERT INTO $fqtn SELECT" +
          s" 3, 3.3, Array('2044-11-11', '2088-12-12'), Map(3,'2044-11-11')," +
          s" Struct(3.3, '2044-11-11'), Array('2019-02-18 15:31:55.333', null)")


      // =======================================================
      // ======= Column tables row buffer and column batch =====
      // =======================================================
      // 5: null and not null atomic data only 1 bucket deletes (in both areas)
      fqtn = "gemfire10.t5"
      stmt.executeUpdate(
        s"""CREATE TABLE $fqtn (c1 integer, c2 double NOT NULL,c3 integer) USING
           | COLUMN options (buckets '1', COLUMN_MAX_DELTA_ROWS '3')""".stripMargin)
      stmt.execute(s"INSERT INTO $fqtn VALUES (1, 111111111111111111.11111111111111111111, 1)")
      stmt.execute(s"INSERT INTO $fqtn VALUES (2, 222222222222222222.22222222222222222222, 2)")
      stmt.execute(s"INSERT INTO $fqtn VALUES (3, 333333333333333333.33333333333333333333, null)")
      stmt.execute(s"INSERT INTO $fqtn VALUES (4, 444444444444444444.44444444444444444444, 4)")
      stmt.execute(s"INSERT INTO $fqtn VALUES (5, 555555555555555555.55555555555555555555, null)")
      stmt.execute(s"DELETE FROM $fqtn WHERE c1 = 2")
      stmt.execute(s"DELETE FROM $fqtn WHERE c1 = 5")

      // 6: null and not null atomic data only 1 bucket deletes/updates (in both areas)
      fqtn = "gemfire10.t6"
      stmt.executeUpdate(
        s"""CREATE TABLE $fqtn (c1 integer, c2 double NOT NULL,c3 integer) USING
           | COLUMN options (buckets '1', COLUMN_MAX_DELTA_ROWS '3')""".stripMargin)
      stmt.execute(s"INSERT INTO $fqtn VALUES (1, 111111111111111111.11111111111111111111, 1)")
      stmt.execute(s"INSERT INTO $fqtn VALUES (2, 222222222222222222.22222222222222222222, null)")
      stmt.execute(s"INSERT INTO $fqtn VALUES (3, 333333333333333333.33333333333333333333, 3)")
      stmt.execute(s"INSERT INTO $fqtn VALUES (4, 444444444444444444.44444444444444444444, null)")
      stmt.execute(s"INSERT INTO $fqtn VALUES (5, 555555555555555555.55555555555555555555, 5)")
      stmt.execute(s"DELETE FROM $fqtn WHERE c1 = 2")
      stmt.execute(s"DELETE FROM $fqtn WHERE c1 = 5")
      stmt.execute(s"UPDATE $fqtn SET c3 = 0 WHERE c1 = 1")
      stmt.execute(s"UPDATE $fqtn SET c3 = 0 WHERE c1 = 4")

      // 7: null and not null atomic data only 1 bucket updates (in both areas)
      fqtn = "gemfire10.t7"
      stmt.executeUpdate(
        s"""CREATE TABLE $fqtn (c1 integer, c2 double NOT NULL,c3 integer) USING
           | COLUMN options (buckets '1', COLUMN_MAX_DELTA_ROWS '3')""".stripMargin)
      stmt.execute(s"INSERT INTO $fqtn VALUES (1, 111111111111111111.11111111111111111111, 1)")
      stmt.execute(s"INSERT INTO $fqtn VALUES (2, 222222222222222222.22222222222222222222, null)")
      stmt.execute(s"INSERT INTO $fqtn VALUES (3, 333333333333333333.33333333333333333333, 3)")
      stmt.execute(s"INSERT INTO $fqtn VALUES (4, 444444444444444444.44444444444444444444, 4)")
      stmt.execute(s"INSERT INTO $fqtn VALUES (5, 555555555555555555.55555555555555555555, null)")
      stmt.execute(s"UPDATE $fqtn SET c3 = 0 WHERE c1 = 1")
      stmt.execute(s"UPDATE $fqtn SET c3 = 0 WHERE c1 = 4")

      // 8: null and not null complex types 1 buckets
      fqtn = "gemfire10.t8"
      stmt.execute(s"""CREATE TABLE $fqtn (c1 integer, c2 double,c3 Array<timestamp>,
           | c4 Map<timestamp,date> NOT NULL, c5 Struct<f1:timestamp, f2:date>,
           | c6 Map<date, timestamp>) USING COLUMN
           |  options (buckets '1', COLUMN_MAX_DELTA_ROWS '2')""".stripMargin)
      stmt.execute(s"""INSERT INTO $fqtn SELECT
                      |1, 1.1, Array(null,'2019-02-18 15:31:55.333','2022-02-22 22:22:22.222'),
                      | Map(cast('2019-02-18 15:31:55.333' as timestamp),cast(null as date))
                      |, Struct('2022-02-22 22:22:22.222', cast(null as date)), Map(cast
                      |('2022-02-22' as date),
                      |cast('2019-02-18 15:31:55.333' as timestamp))""".stripMargin)
      stmt.execute(s"""INSERT INTO $fqtn SELECT
                      |2, 2.2, Array('2111-11-11 15:31:55.333', '2022-02-22 44:33:55.666', null),
                      | Map(cast('2111-11-11 15:31:55.333' as timestamp),cast('2011-11-11' as
                      | date)), Struct('2022-02-22 22:22:22.222', cast(null as date)),
                      |  Map(cast('2011-11-11' as date),cast('2111-11-11 15:31:55.333' as
                      |  timestamp))"""
          .stripMargin)
      stmt.execute(s"""INSERT INTO $fqtn SELECT
                      |3, 3.3, Array('2022-11-18 11:31:11.333', '2022-02-11 22:11:22.111'),
                      |Map(cast('2022-11-18 11:31:11.333' as timestamp),cast('2044-11-11' as
                      | date)), Struct(cast(null as timestamp), '2011-11-11'),
                      | Map(cast('2044-11-11' as date),
                      |cast('2022-11-18 11:31:11.333' as timestamp))""".stripMargin)

      // ===================================
      // ======= Row table partitioned =====
      // ===================================
      // 9: null and not null atomic data only 1 bucket update/delete alter add/drop/add
      fqtn = "gemfire10.t9"
      stmt.executeUpdate(
        s"""CREATE TABLE $fqtn (c1 integer, c2 double NOT NULL,c3 integer)
           | USING row options (partition_by 'c1', buckets '1')""".stripMargin)
      stmt.execute(s"INSERT INTO $fqtn VALUES (1, 111111111111111111.11111111111111111111, 1)")
      stmt.execute(s"INSERT INTO $fqtn VALUES (2, 222222222222222222.22222222222222222222, null)")
      stmt.execute(s"INSERT INTO $fqtn VALUES (3, 333333333333333333.33333333333333333333, 3)")
      stmt.execute(s"INSERT INTO $fqtn VALUES (4, 444444444444444444.44444444444444444444, null)")
      stmt.execute(s"INSERT INTO $fqtn VALUES (5, 555555555555555555.55555555555555555555, 5)")
      stmt.execute(s"UPDATE $fqtn SET c3 = 0 WHERE c1 = 1")
      stmt.execute(s"UPDATE $fqtn SET c3 = 0 WHERE c1 = 4")
      stmt.execute(s"ALTER TABLE $fqtn DROP COLUMN c2")
      stmt.execute(s"DELETE FROM $fqtn WHERE c1 = 2")
      stmt.execute(s"DELETE FROM $fqtn WHERE c1 = 5")
      stmt.execute(s"ALTER TABLE $fqtn ADD COLUMN c4 integer")
      stmt.execute(s"INSERT INTO $fqtn VALUES (9, 99, 999)")

      // 10: null and not null complex types 2 buckets no alter
      fqtn = "gemfire10.t10"
      stmt.executeUpdate(
        s"""CREATE TABLE $fqtn (c1 integer, c2 double NOT NULL,c3 integer)
           | USING row options (partition_by 'c1', buckets '2')""".stripMargin)
      stmt.execute(s"INSERT INTO $fqtn VALUES (1, 111111111111111111.11111111111111111111, 1)")
      stmt.execute(s"INSERT INTO $fqtn VALUES (2, 222222222222222222.22222222222222222222, null)")
      stmt.execute(s"INSERT INTO $fqtn VALUES (3, 333333333333333333.33333333333333333333, 3)")
      stmt.execute(s"INSERT INTO $fqtn VALUES (4, 444444444444444444.44444444444444444444, null)")
      stmt.execute(s"INSERT INTO $fqtn VALUES (5, 555555555555555555.55555555555555555555, 5)")


      // ===================================
      // ======= Row table replicated ======
      // ===================================
      // 11: null and not null atomic data only update/delete alter add/drop/add
      fqtn = "gemfire10.t11"
      stmt.executeUpdate(
        s"""CREATE TABLE $fqtn (c1 integer, c2 double NOT NULL,c3 integer) USING row""".stripMargin)
      stmt.execute(s"INSERT INTO $fqtn VALUES (1, 111111111111111111.11111111111111111111, 1)")
      stmt.execute(s"INSERT INTO $fqtn VALUES (2, 222222222222222222.22222222222222222222, null)")
      stmt.execute(s"INSERT INTO $fqtn VALUES (3, 333333333333333333.33333333333333333333, 3)")
      stmt.execute(s"UPDATE $fqtn SET c3 = 0 WHERE c1 = 1")
      stmt.execute(s"ALTER TABLE $fqtn DROP COLUMN c2")
      stmt.execute(s"DELETE FROM $fqtn WHERE c1 = 2")
      stmt.execute(s"ALTER TABLE $fqtn ADD COLUMN c2 integer")
      stmt.execute(s"INSERT INTO $fqtn VALUES (9, 99, 999)")

      // covers bulk updates, deletes
      fqtn = "gemfire10.t13"
      stmt.execute(s"""CREATE TABLE $fqtn(
                     BNK_ORG_ID BIGINT NOT NULL,
                     BNK_ID BIGINT NOT NULL,
                     VER BIGINT NOT NULL,
                     CLIENT_ID BIGINT NOT NULL,
                     BNK_FULL_NM VARCHAR(50),
                     RTNG_NUM VARCHAR(35) NOT NULL,
                     VLD_FRM_DT TIMESTAMP NOT NULL,
                     VLD_TO_DT TIMESTAMP,
                     SRC_SYS_REF_ID VARCHAR(10) NOT NULL,
                     SRC_SYS_REC_ID VARCHAR(150)) USING column OPTIONS(partition_by 'BNK_ORG_ID',
                     buckets '32',key_columns 'CLIENT_ID,BNK_ORG_ID,BNK_ID ',
                     redundancy '1') """)
      stmt.execute(s"""INSERT into $fqtn select id,id,abs(rand()*1000),abs(rand()*1000),
                'BNK_FULL_NM','RTNG_NUM',from_unixtime(unix_timestamp('2018-01-01 01:00:00')
                +floor(rand()*31536000)),from_unixtime(unix_timestamp('2019-01-01 01:00:00')
                +floor(rand()*31536000)),'src_sys_ref_id','src_sys_rec_id' from range(7000000)""")

      stmt.execute(s"UPDATE $fqtn set ver = ver * 2 where bnk_id % 2 = 0")
      stmt.execute(s"DELETE from $fqtn where bnk_id % 3 = 0;")

      val rst13 = stmt.executeQuery(s"SELECT * FROM $fqtn ORDER BY 1")
      compareResultSet(fqtn, rst13, false)
      rst13.close()

      stmt.close()
      conn.close()

      stopCluster()
      startSnappyRecoveryCluster()

      var connRec: Connection = null
      var stmtRec: Statement = null
      var str = new mutable.StringBuilder()
      val arrBuf: ArrayBuffer[String] = ArrayBuffer.empty

      logInfo("============ Recovery mode ============")
      connRec = getConn(locNetPort, "gemfire10", "gemfire10")
      stmtRec = connRec.createStatement()
      Thread.sleep(3000)

      def getRecFromResultSet(rs: ResultSet, schemaStr: String): ListBuffer[Array[Any]] = {
        var result = new ListBuffer[Array[Any]]()
        while (rs.next()) {
          var i = 1
          val recArr = schemaStr.split(",").map(_.toLowerCase).map(f => {
            val fValue = f match {
              case "integer" | "int" => rs.getInt(i)
              case "double" => rs.getDouble(i)
              case "array" | "map" | "struct" => rs.getString(i)
              case _ => rs.getString(i)
            }
            i += 1
            if (rs.wasNull()) null else fValue
          })
          logInfo(s"recarr = ${recArr.toSeq}")
          result += recArr
        }
        result
      }

      def compareResult(expectedResult: ListBuffer[Array[Any]],
          result: ListBuffer[Array[Any]]): Unit = {
        for (rec <- result) {
          for (expRec <- expectedResult) {
            if (expRec.sameElements(rec)) result.remove(result.indexOf(rec))
            else logInfo(s"expRec != rec. ${expRec.toSeq} != ${rec.toSeq}")
          }
        }
        assert(result.size == 0, s"result has extra records")
      }

      // *********************************************
      // ****************** testcase 1 ***************
      // *********************************************
      var rs = stmtRec.executeQuery("select * from gemfire10.t1")
      val expectedResult1: ListBuffer[Array[Any]] = ListBuffer(
        Array(1, 111111111111111111.11111111111111111111, 11),
        Array(2, 222222222222222222.22222222222222222222, null),
        Array(3, 322222222222222222.22222222222222222222, 33),
        Array(4, 422222222222222222.22222222222222222222, null)
      )
      compareResult(expectedResult1,
        getRecFromResultSet(rs, "integer,double,integer"))
      rs.close()

      // testcase 2
      rs = stmtRec.executeQuery("select * from gemfire10.t2")
      rs.next()
      val expectedResult2: ListBuffer[Array[Any]] = ListBuffer(
        Array(1, 1.1,
          """{"col_0":["2011-11-11","2010-12-12",null]}""",
          """{"col_1":{"1":"2011-11-11"}}""",
          """{"col_2":{"f1":1.1,"f2":"2011-11-11"}}""",
          """{"col_3":[null,"2022-02-22T22:22:22.222+05:30"]}"""),
        Array(2, null,
          """{"col_0":[null,"2022-11-11","2044-06-06","2006-06-05"]}""",
          """{"col_1":{"2":null}}""",
          """{"col_2":{"f1":2.2}}""",
          """{"col_3":["2019-02-18T15:31:55.333+05:30","2022-02-22T22:22:22.222+05:30"]}"""),
        Array(3, 3.3,
          """{"col_0":["2044-11-11","2088-12-12"]}""",
          """{"col_1":{"3":"2044-11-11"}}""",
          """{"col_2":{"f1":3.3,"f2":"2044-11-11"}}""",
          """{"col_3":["2019-02-18T15:31:55.333+05:30",null]}""")
      )
      compareResult(expectedResult2,
        getRecFromResultSet(rs, "integer,double,array,map,struct,array"))
      rs.close()

      // testcase 3
      rs = stmtRec.executeQuery("select * from gemfire10.t3")
      val expectedResult3: ListBuffer[Array[Any]] = ListBuffer(
        Array(1, 111111111111111111.11111111111111111111, null),
        Array(2, 222222222222222222.22222222222222222222, 22),
        Array(3, 322222222222222222.22222222222222222222, 33)
      )
      compareResult(expectedResult3,
        getRecFromResultSet(rs, "integer,double,integer"))
      rs.close()

      // testcase 4
      rs = stmtRec.executeQuery("select * from gemfire10.t4")
      rs.next()
      val expectedResult4: ListBuffer[Array[Any]] = ListBuffer(
        Array(1, 1.1,
          """{"col_0":[null,"2022-11-11","2044-06-06","2006-06-05"]}""",
          """{"col_1":{"1":null}}""",
          """{"col_2":{"f1":1.1}}""",
          """{"col_3":[null,"2022-02-22T22:22:22.222+05:30"]}"""),
        Array(2, 2.2,
          """{"col_0":["2011-11-11","2010-12-12",null]}""",
          """{"col_1":{"2":"2011-11-11"}}""",
          """{"col_2":{"f1":2.2,"f2":"2011-11-11"}}""",
          """{"col_3":["2019-02-18T15:31:55.333+05:30","2022-02-22T22:22:22.222+05:30"]}"""),
        Array(3, 3.3,
          """{"col_0":["2044-11-11","2088-12-12"]}""",
          """{"col_1":{"3":"2044-11-11"}}""",
          """{"col_2":{"f1":3.3,"f2":"2044-11-11"}}""",
          """{"col_3":["2019-02-18T15:31:55.333+05:30",null]}""")
      )
      compareResult(expectedResult4,
        getRecFromResultSet(rs, "integer,double,array,map,struct,array"))
      rs.close()

      // testcase 5
      rs = stmtRec.executeQuery("select * from gemfire10.t5")
      val expectedResult5: ListBuffer[Array[Any]] = ListBuffer(
        Array(1, 111111111111111111.11111111111111111111, 1),
        Array(3, 333333333333333333.33333333333333333333, null),
        Array(4, 444444444444444444.44444444444444444444, 4)
      )
      compareResult(expectedResult5,
        getRecFromResultSet(rs, "integer,double,integer"))
      rs.close()

      // testcase 6
      rs = stmtRec.executeQuery("select * from gemfire10.t6")
      val expectedResult6: ListBuffer[Array[Any]] = ListBuffer(
        Array(1, 111111111111111111.11111111111111111111, 0),
        Array(3, 333333333333333333.33333333333333333333, 3),
        Array(4, 444444444444444444.44444444444444444444, 0)
      )
      compareResult(expectedResult6,
        getRecFromResultSet(rs, "integer,double,integer"))
      rs.close()

      // 7: null and not null atomic data only 1 bucket updates (in both areas)
      rs = stmtRec.executeQuery("select * from gemfire10.t7")
      val expectedResult7: ListBuffer[Array[Any]] = ListBuffer(
        Array(1, 111111111111111111.11111111111111111111, 0),
        Array(2, 222222222222222222.22222222222222222222, null),
        Array(3, 333333333333333333.33333333333333333333, 3),
        Array(4, 444444444444444444.44444444444444444444, 0),
        Array(5, 555555555555555555.55555555555555555555, null)
      )
      compareResult(expectedResult7,
        getRecFromResultSet(rs, "integer,double,integer"))
      rs.close()

      // 8: null and not null complex types 2 buckets
      rs = stmtRec.executeQuery("select * from gemfire10.t8")
      val expectedResult8: ListBuffer[Array[Any]] = ListBuffer(
        Array(2, 2.2,
          """{"col_0":["2111-11-11T15:31:55.333+05:30",null,null]}""",
          """{"col_1":{"4476679315333000":"2011-11-11"}}""",
          """{"col_2":{"f1":"2022-02-22T22:22:22.222+05:30"}}""",
          """{"col_3":{"15289":"2111-11-11T15:31:55.333+05:30"}}"""),
        Array(1, 1.1,
          """{"col_0":[null,"2019-02-18T15:31:55.333+05:30","2022-02-22T22:22:22.222+05:30"]}""",
          """{"col_1":{"1550484115333000":null}}""",
          """{"col_2":{"f1":"2022-02-22T22:22:22.222+05:30"}}""",
          """{"col_3":{"19045":"2019-02-18T15:31:55.333+05:30"}}"""),
        Array(3, 3.3,
          """{"col_0":["2022-11-18T11:31:11.333+05:30","2022-02-11T22:11:22.111+05:30"]}""",
          """{"col_1":{"1668751271333000":"2044-11-11"}}""",
          """{"col_2":{"f2":"2011-11-11"}}""",
          """{"col_3":{"27343":"2022-11-18T11:31:11.333+05:30"}}""")
      )
      compareResult(expectedResult8,
        getRecFromResultSet(rs, "integer,double,array,map,struct,map"))
      rs.close()

      // 9: null and not null atomic data only 1 bucket update/delete alter add/drop/add
      rs = stmtRec.executeQuery("select * from gemfire10.t9")
      val expectedResult9: ListBuffer[Array[Any]] = ListBuffer(
        Array(1, 0, null),
        Array(3, 3, null),
        Array(4, 0, null),
        Array(9, 99, 999)
      )
      compareResult(expectedResult9,
        getRecFromResultSet(rs, "integer,integer,integer"))
      rs.close()

      //      // 10: null and not null complex types 2 buckets no alter
      //      val rs10 = stmtRec.executeQuery("select * from gemfire10.t10")
      //      val expectedResult10: ListBuffer[Array[Any]] = ListBuffer(
      //        Array(1, 111111111111111111.11111111111111111111, 1),
      //        Array(2, 222222222222222222.22222222222222222222, null),
      //        Array(3, 333333333333333333.33333333333333333333, 3),
      //        Array(4, 444444444444444444.44444444444444444444, null),
      //        Array(5, 555555555555555555.55555555555555555555, 5)
      //      )
      //      compareResult(expectedResult10,
      //        getRecFromResultSet(rs10, "integer,integer,integer"))
      //      rs10.close()

      // 11: null and not null atomic data only update/delete alter add/drop/add
      rs = stmtRec.executeQuery("select * from gemfire10.t11")
      val expectedResult11: ListBuffer[Array[Any]] = ListBuffer(
        Array(1, 0, null),
        Array(3, 3, null),
        Array(9, 99, 999)
      )
      compareResult(expectedResult11,
        getRecFromResultSet(rs, "integer,integer,integer"))
      rs.close()

      rs = stmtRec.executeQuery(s"SELECT * FROM gemfire10.t13 ORDER BY 1")
      compareResultSet(fqtn, rs, true)
      rs.close()

      stmtRec.execute("call sys.EXPORT_DDLS('./recover_ddls/');")
      // todo hmeka Add assertion on recover_ddls output
      stmtRec.close()
      connRec.close()

      afterEach()
      test_status = true
    } catch {
      case e: Throwable =>
        test_status = false
        throw new Exception(e)
    } finally {
      afterEach()
    }
  }
}

object PrimaryDUnitRecoveryTest extends Logging {
  var snappyHome = ""

  var ldapProperties: Properties = new Properties()

  def getLdapConf: String = {
    var conf = ""
    for (k <- List(Attribute.AUTH_PROVIDER, Attribute.USERNAME_ATTR, Attribute.PASSWORD_ATTR)) {
      conf += s"-$k=${PrimaryDUnitRecoveryTest.ldapProperties.getProperty(k)} "
    }
    for (k <- List(AUTH_LDAP_SERVER, AUTH_LDAP_SEARCH_BASE)) {
      conf += s"-J-D$k=${PrimaryDUnitRecoveryTest.ldapProperties.getProperty(k)} "
    }
    conf
  }

  def getJdbcConnection(netPort: Int): Connection = {
    val driver = "io.snappydata.jdbc.ClientDriver"
    Utils.classForName(driver).newInstance
    var url: String = "jdbc:snappydata://localhost:" + netPort + "/"
    DriverManager.getConnection(url)
  }

  // CWD will be assumed the same for all command which is $snappyHome
  def executeCommand(command: String): (String, String) = {
    val stdoutStream = new ByteArrayOutputStream
    val stderrStream = new ByteArrayOutputStream

    val teeOut = new TeeOutputStream(stdout, new BufferedOutputStream(stdoutStream))
    val teeErr = new TeeOutputStream(stderr, new BufferedOutputStream(stderrStream))

    val stdoutWriter = new PrintStream(teeOut, true)
    val stderrWriter = new PrintStream(teeErr, true)

    val code = Process(command, new File(s"$snappyHome")) !
        //    scalastyle:off println
        ProcessLogger(stdoutWriter.println, stderrWriter.println)
    //    scalastyle:on println

    var stdoutStr = stdoutStream.toString
    if (code != 0) {
      // add an exception to the output to force failure
      stdoutStr += s"\n***** Exit with Exception code = $code\n"
    }
    (stdoutStr, stderrStream.toString)
  }
}
