package acolyte.jdbc

import java.util.Properties

import java.sql.{
  ResultSet,
  SQLException,
  BatchUpdateException,
  SQLFeatureNotSupportedException,
  Statement
}
import Statement.EXECUTE_FAILED

import org.specs2.mutable.Specification

import acolyte.jdbc.test.{ EmptyConnectionHandler, Params }

object AbstractStatementSpec extends Specification {
  "Abstract statement specification" title

  "Constructor" should {
    "refuse null connection" in {
      statement(c = null) aka "ctor" must throwA[IllegalArgumentException](
        message = "Invalid connection")
    }

    "refuse null handler" in {
      statement(h = null).
        aka("ctor") must throwA[IllegalArgumentException](
          message = "Invalid handler")
    }
  }

  "Wrapping" should {
    "be valid for java.sql.Statement" in {
      statement().isWrapperFor(classOf[Statement]).
        aka("is wrapper for java.sql.Statement") must beTrue

    }

    "be unwrapped to java.sql.Statement" in {
      Option(statement().unwrap(classOf[Statement])).
        aka("unwrapped") must beSome.which(_.isInstanceOf[Statement])

    }
  }

  "Query execution" should {
    var sql: String = null
    lazy val h = new StatementHandler {
      def isQuery(s: String) = true
      def whenSQLUpdate(s: String, p: Params) = UpdateResult.Nothing
      def whenSQLQuery(s: String, p: Params) = {
        sql = s
        RowLists.stringList.asResult
      }
    }

    "return empty resultset" in {
      lazy val s = statement(h = h)
      lazy val rows = RowLists.stringList

      (s.executeQuery("QUERY") aka "result" mustEqual rows.resultSet).
        and(s.getResultSet aka "resultset" mustEqual rows.resultSet).
        and(s.getResultSet.getStatement aka "statement" mustEqual s).
        and(s.getUpdateCount aka "update count" mustEqual -1).
        and(sql aka "executed SQL" mustEqual "QUERY")

    }

    "fail on a closed statement" in {
      lazy val s = statement()
      s.close()

      s.executeQuery("QUERY") aka "query" must throwA[SQLException](
        message = "Statement is closed")

    }

    "be processed" in {
      lazy val s = statement(h = h)

      (s.execute("QUERY") aka "flag" must beTrue).
        and(s.getResultSet aka "resultset" mustEqual RowLists.stringList.resultSet).
        and(s.getUpdateCount aka "update count" mustEqual -1).
        and(sql aka "executed SQL" mustEqual "QUERY")

    }

    "be processed ignoring generated keys" in {
      lazy val s = statement(h = h)

      ((s.execute("QUERY", Statement.RETURN_GENERATED_KEYS).
        aka("flag") must beTrue).
        and(s.getResultSet aka "resultset" mustEqual RowLists.stringList.resultSet).
        and(s.getUpdateCount aka "update count" mustEqual -1).
        and(sql aka "executed SQL" mustEqual "QUERY")).
        /*2*/ and((s.execute("QUERY", Array[Int]()) aka "flag" must beTrue).
          and(s.getResultSet aka "resultset" mustEqual RowLists.stringList.resultSet).
          and(s.getUpdateCount aka "update count" mustEqual -1).
          and(sql aka "executed SQL" mustEqual "QUERY")).
        /*3*/ and((s.execute("QUERY", Array[String]()) aka "flag" must beTrue).
          and(s.getResultSet aka "resultset" mustEqual RowLists.stringList.resultSet).
          and(s.getUpdateCount aka "update count" mustEqual -1).
          and(sql aka "executed SQL" mustEqual "QUERY"))

    }

    "be processed without generated keys" in {
      lazy val s = statement(h = h)

      (s.execute("QUERY", Statement.NO_GENERATED_KEYS).
        aka("flag") must beTrue).
        and(s.getResultSet aka "resultset" mustEqual RowLists.stringList.resultSet).
        and(s.getUpdateCount aka "update count" mustEqual -1).
        and(sql aka "executed SQL" mustEqual "QUERY")

    }

    "fail on runtime exception" in {
      lazy val h = new StatementHandler {
        def isQuery(s: String) = true
        def whenSQLUpdate(s: String, p: Params) = UpdateResult.Nothing
        def whenSQLQuery(s: String, p: Params) = sys.error("Unexpected")
      }

      statement(h = h).executeQuery("SELECT").
        aka("execution") must throwA[SQLException](message = "Unexpected")
    }
  }

  "Update execution" should {
    lazy val genKeys = RowLists.intList.append(2).append(5)
    var sql: String = null
    lazy val h = new StatementHandler {
      def isQuery(s: String) = false
      def whenSQLUpdate(s: String, p: Params) = {
        sql = s; new UpdateResult(5).withGeneratedKeys(genKeys)
      }
      def whenSQLQuery(s: String, p: Params) = sys.error("TEST")
    }

    "return expected row count" in {
      lazy val s = statement(h = h)

      (s.executeUpdate("UPDATE") aka "result" mustEqual 5).
        and(s.getUpdateCount aka "update count" mustEqual 5).
        and(s.getResultSet aka "resultset" must beNull).
        and(s.getGeneratedKeys aka "generated keys" must beLike {
          case ks ⇒ (ks.next aka "has first key" must beTrue).
            and(ks.getInt(1) aka "first key" must_== 2).
            and(ks.next aka "has second key" must beTrue).
            and(ks.getInt(1) aka "second key" must_== 5).
            and(ks.next aka "has third key" must beFalse)
        }).and(sql aka "executed SQL" mustEqual "UPDATE")

    }

    "return expected row count without generated keys" in {
      lazy val s = statement(h = h)

      (s.executeUpdate("UPDATE", Statement.NO_GENERATED_KEYS).
        aka("result") mustEqual 5).
        and(s.getUpdateCount aka "update count" mustEqual 5).
        and(s.getResultSet aka "resultset" must beNull).
        and(s.getGeneratedKeys aka "generated keys" must beLike {
          case ks ⇒ ks.next aka "has key" must beFalse
        }).and(sql aka "executed SQL" mustEqual "UPDATE")

    }

    "fail on a closed statement" in {
      lazy val s = statement()
      s.close()

      s.executeUpdate("UPDATE") aka "update" must throwA[SQLException](
        message = "Statement is closed")

    }

    "be processed" in {
      lazy val s = statement(h = h)

      (s.execute("UPDATE") aka "result" must beFalse).
        and(s.getUpdateCount aka "update count" mustEqual 5).
        and(s.getResultSet aka "resultset" must beNull).
        and(s.getGeneratedKeys aka "gen keys" mustEqual genKeys.resultSet).
        and(sql aka "executed SQL" mustEqual "UPDATE")

    }

    "be processed without generated keys" in {
      lazy val s = statement(h = h)

      (s.execute("UPDATE", Statement.NO_GENERATED_KEYS).
        aka("result") must beFalse).
        and(s.getUpdateCount aka "update count" mustEqual 5).
        and(s.getResultSet aka "resultset" must beNull).
        and(s.getGeneratedKeys aka "generated keys" mustEqual (
          RowLists.stringList.resultSet)).
        and(sql aka "executed SQL" mustEqual "UPDATE")

    }

    "fail on runtime exception" in {
      lazy val h = new StatementHandler {
        def isQuery(s: String) = false
        def whenSQLUpdate(s: String, p: Params) = sys.error("Unexpected")
        def whenSQLQuery(s: String, p: Params) = sys.error("Not query")
      }

      statement(h = h).executeUpdate("UPDATE").
        aka("execution") must throwA[SQLException](message = "Unexpected")
    }
  }

  "Closed statement" should {
    "be marked" in {
      lazy val s = statement()
      s.close()

      s.isClosed aka "flag" should beTrue
    }
  }

  "Statement" should {
    "have expected connection" in {
      statement().getConnection aka "connection" mustEqual defaultCon
    }

    "have no field max size" in {
      (statement().getMaxFieldSize aka "max size" mustEqual 0).
        and(statement().setMaxFieldSize(1).
          aka("setter") must throwA[UnsupportedOperationException])
    }

    "have not query timeout" in {
      (statement().getQueryTimeout aka "timeout" mustEqual 0).
        and(statement().setQueryTimeout(1).
          aka("setter") must throwA[UnsupportedOperationException])

    }

    "not support cancel" in {
      statement().cancel().
        aka("cancel") must throwA[SQLFeatureNotSupportedException]

    }

    "have default resultset holdability" in {
      statement().getResultSetHoldability().
        aka("holdability") mustEqual ResultSet.CLOSE_CURSORS_AT_COMMIT

    }

    "not be poolable" in {
      statement().isPoolable aka "poolable" must beFalse
    }

    "not close on completion" in {
      statement().isCloseOnCompletion aka "close on completion" must beFalse
    }
  }

  "Fetch size" should {
    "initially be zero" in {
      statement().getFetchSize aka "initial size" mustEqual 0
    }

    "not be accessible on a closed statement" in {
      lazy val s = statement()
      s.close()

      (s.getFetchSize aka "getter" must throwA[SQLException](
        message = "Statement is closed")).
        and(s.setFetchSize(1) aka "setter" must throwA[SQLException](
          message = "Statement is closed"))

    }

    "not be set negative" in {
      statement().setFetchSize(-1) aka "setter" must throwA[SQLException](
        message = "Negative fetch size")

    }
  }

  "Max row count" should {
    "initially be zero" in {
      statement().getMaxRows aka "initial count" mustEqual 0
    }

    "not be accessible on a closed statement" in {
      lazy val s = statement()
      s.close()

      (s.getMaxRows aka "getter" must throwA[SQLException](
        message = "Statement is closed")).
        and(s.setMaxRows(1) aka "setter" must throwA[SQLException](
          message = "Statement is closed"))

    }

    "not be set negative" in {
      statement().setMaxRows(-1) aka "setter" must throwA[SQLException](
        message = "Negative max rows")

    }

    "skip row #3 with max count 2" in {
      lazy val h = new StatementHandler {
        def isQuery(s: String) = true
        def whenSQLUpdate(s: String, p: Params) = UpdateResult.Nothing
        def whenSQLQuery(s: String, p: Params) = {
          RowLists.stringList.append("A").append("B").append("C").asResult
        }
      }
      lazy val s = statement(h = h)
      s.setMaxRows(2)

      (s.execute("QUERY") aka "flag" must beTrue).
        and(s.getResultSet aka "resultset" mustEqual {
          RowLists.stringList.append("A").append("B").resultSet
        })
    }
  }

  "Batch" should {
    class Handler extends StatementHandler {
      var exed = Seq[String]()
      def isQuery(s: String) = false
      def whenSQLUpdate(s: String, p: Params) = {
        exed = exed :+ s; new UpdateResult(exed.size)
      }
      def whenSQLQuery(s: String, p: Params) = sys.error("TEST")
    }

    "not be added on closed statement" in {
      lazy val s = statement()
      s.close()

      s.addBatch("UPDATE") aka "add batch" must throwA[SQLException](
        message = "Statement is closed")
    }

    "be executed with 2 elements" in {
      val h = new Handler()
      lazy val s = statement(h = h)
      s.addBatch("BATCH1"); s.addBatch("2_BATCH")

      s.executeBatch() aka "batch execution" mustEqual Array[Int](1, 2) and (
        h.exed aka "executed" must contain(allOf("BATCH1", "2_BATCH").inOrder))
    }

    "throw exception as error is raised while executing first element" in {
      val h = new Handler {
        override def whenSQLUpdate(s: String, p: Params) =
          sys.error("Batch error")
      }
      lazy val s = statement(h = h)
      s.addBatch("BATCH1"); s.addBatch("2_BATCH")

      s.executeBatch() aka "batch execution" must throwA[BatchUpdateException].
        like {
          case ex: BatchUpdateException ⇒
            (ex.getUpdateCounts aka "update count" must_== Array[Int](
              EXECUTE_FAILED, EXECUTE_FAILED)).
              and(ex.getCause.getMessage aka "cause" mustEqual "Batch error")
        }
    }

    "continue after error on first element (batch.continueOnError)" in {
      val props = new Properties()
      props.put("acolyte.batch.continueOnError", "true")

      var i = 0
      val h = new Handler {
        override def whenSQLUpdate(s: String, p: Params) = {
          i = i + 1
          if (i == 1) sys.error(s"Batch error: $i")
          new UpdateResult(i)
        }
      }
      lazy val s =
        statement(new acolyte.jdbc.Connection(jdbcUrl, props, defaultHandler), h)
      s.addBatch("BATCH1"); s.addBatch("2_BATCH")

      s.executeBatch() aka "batch execution" must throwA[BatchUpdateException].
        like {
          case ex: BatchUpdateException ⇒
            (ex.getUpdateCounts aka "update count" must_== Array[Int](
              EXECUTE_FAILED, 2)).
              and(ex.getCause.getMessage aka "cause" mustEqual "Batch error: 1")
        }
    }

    "throw exception as error is raised while executing second element" in {
      var i = 0
      val h = new Handler {
        override def whenSQLUpdate(s: String, p: Params) = {
          i = i + 1
          if (i == 2) sys.error(s"Batch error: $i")
          new UpdateResult(i)
        }
      }
      lazy val s = statement(h = h)
      s.addBatch("BATCH1"); s.addBatch("2_BATCH")

      s.executeBatch() aka "batch execution" must throwA[BatchUpdateException].
        like {
          case ex: BatchUpdateException ⇒
            (ex.getUpdateCounts aka "update count" must_== Array[Int](
              1, EXECUTE_FAILED)).
              and(ex.getCause.getMessage aka "cause" mustEqual "Batch error: 2")
        }
    }

    "throw exception executing second element (batch.continueOnError)" in {
      val props = new Properties()
      props.put("acolyte.batch.continueOnError", "true")

      var i = 0
      val h = new Handler {
        override def whenSQLUpdate(s: String, p: Params) = {
          i = i + 1
          if (i == 2) sys.error(s"Batch error: $i")
          new UpdateResult(i)
        }
      }
      lazy val s =
        statement(new acolyte.jdbc.Connection(jdbcUrl, props, defaultHandler), h)
      s.addBatch("BATCH1"); s.addBatch("2_BATCH")

      s.executeBatch() aka "batch execution" must throwA[BatchUpdateException].
        like {
          case ex: BatchUpdateException ⇒
            (ex.getUpdateCounts aka "update count" must_== Array[Int](
              1, EXECUTE_FAILED)).
              and(ex.getCause.getMessage aka "cause" mustEqual "Batch error: 2")
        }
    }

    "be cleared and not executed" in {
      val h = new Handler()
      lazy val s = statement(h = h)
      s.addBatch("BATCH1"); s.addBatch("2_BATCH")

      s.clearBatch() aka "clear batch" must not(throwA[SQLException]) and (
        h.exed.size aka "executed" must_== 0)
    }
  }

  "Generated keys" should {
    "be initially empty" in {
      statement().getGeneratedKeys.
        aka("keys") mustEqual RowLists.stringList.resultSet

    }

    "not be returned from update" in {
      (statement().executeUpdate("UPDATE", Array[Int]()).
        aka("update 2") must throwA[SQLFeatureNotSupportedException]).
        and(statement().executeUpdate("UPDATE", Array[String]()).
          aka("update 3") must throwA[SQLFeatureNotSupportedException])

    }

    "not be returned from execution" in {
      (statement().execute("UPDATE", Array[Int]()).
        aka("execute 1") must throwA[SQLFeatureNotSupportedException]).
        and(statement().execute("UPDATE", Array[String]()).
          aka("execute 2") must throwA[SQLFeatureNotSupportedException])

    }
  }

  "Warning" should {
    lazy val warning = new java.sql.SQLWarning("TEST")

    "initially be null" in {
      statement().getWarnings aka "warning" must beNull
    }

    "be found for query" in {
      lazy val h = new StatementHandler {
        def isQuery(s: String) = true
        def whenSQLUpdate(s: String, p: Params) = sys.error("Not")
        def whenSQLQuery(s: String, p: Params) =
          RowLists.stringList.asResult.withWarning(warning)

      }

      lazy val s = statement(h = h)
      s.executeQuery("TEST")

      (s.getWarnings aka "warning" mustEqual warning).
        and(Option(s.getResultSet) aka "resultset" must beSome.which {
          _.getWarnings aka "result warning" mustEqual warning
        })
    }

    "be found for update" in {
      lazy val h = new StatementHandler {
        def isQuery(s: String) = false
        def whenSQLQuery(s: String, p: Params) = sys.error("Not")
        def whenSQLUpdate(s: String, p: Params) =
          UpdateResult.Nothing.withWarning(warning)

      }

      lazy val s = statement(h = h)
      s.executeUpdate("TEST")

      s.getWarnings aka "warning" mustEqual warning
    }
  }

  // ---

  def statement(c: Connection = defaultCon, h: StatementHandler = defaultHandler.getStatementHandler) = new AbstractStatement(c, h) {}

  val jdbcUrl = "jdbc:acolyte:test"
  lazy val defaultCon =
    new acolyte.jdbc.Connection(jdbcUrl, null, defaultHandler)
  lazy val defaultHandler = EmptyConnectionHandler
}
