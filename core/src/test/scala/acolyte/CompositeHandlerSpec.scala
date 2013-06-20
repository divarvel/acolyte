package acolyte

import org.specs2.mutable.Specification

import acolyte.AbstractStatement.NO_PARAMS
import acolyte.StatementHandler.Parameter
import acolyte.CompositeHandler.{ QueryHandler, UpdateHandler }

object CompositeHandlerSpec extends Specification {
  "Composite statement handler" title

  "Query detection" should {
    "not be inited" in {
      new CompositeHandler().withQueryDetection(null.
        asInstanceOf[java.util.regex.Pattern]).
        aka("init") must throwA[IllegalArgumentException]

    }

    "always match" in {
      lazy val h = new CompositeHandler().withQueryDetection(".*")

      (h.isQuery("TEST") aka "detection 1" must beTrue).
        and(h.isQuery("SELECT * FROM table") aka "detection 2" must beTrue)
    }

    "match with a single pattern" in {
      new CompositeHandler().withQueryDetection("^SELECT ").
        isQuery("SELECT * FROM table") aka "detection" must beTrue

    }

    "not match" in {
      new CompositeHandler().withQueryDetection("^SELECT ").
        isQuery("TEST") aka "detection" must beFalse

    }

    "match with multiple patterns" in {
      new CompositeHandler().withQueryDetection("^SELECT ").
        withQueryDetection("EXEC that_proc").
        isQuery("EXEC that_proc('test')") aka "detection" must beTrue

    }
  }

  "Update handling" should {
    "not be inited" in {
      new CompositeHandler().withUpdateHandler(null.asInstanceOf[UpdateHandler]).
        aka("init") must throwA[IllegalArgumentException]

    }

    "be successful" in {
      (new CompositeHandler().withUpdateHandler(new UpdateHandler {
        def apply(s: String, p: java.util.List[Parameter]) = 1
      }).whenSQLUpdate("TEST", NO_PARAMS) aka "count" mustEqual 1).
        and(new CompositeHandler().withUpdateHandler(new UpdateHandler {
          def apply(s: String, p: java.util.List[Parameter]) = 3
        }).whenSQLUpdate("TEST", NO_PARAMS) aka "count" mustEqual 3).
        and(new CompositeHandler().withUpdateHandler(new UpdateHandler {
          def apply(s: String, p: java.util.List[Parameter]) = 10
        }).whenSQLUpdate("TEST", NO_PARAMS) aka "count" mustEqual 10)

    }
  }

  "Query handling" should {
    "not be inited" in {
      new CompositeHandler().withQueryHandler(null.asInstanceOf[QueryHandler]).
        aka("init") must throwA[IllegalArgumentException]

    }

    "be successful for empty resultset" in {
      new CompositeHandler().withQueryHandler(new QueryHandler {
        def apply(s: String, p: java.util.List[Parameter]) =
          RowLists.rowList1(classOf[String]).resultSet

      }).whenSQLQuery("SELECT *", NO_PARAMS).
        aka("resultset") mustEqual RowLists.rowList1(classOf[String]).resultSet
    }

    "be successful for not-empty resultset" in {
      lazy val x: RowList2[String,Float] = new RowList2(
        classOf[String], classOf[Float]).
        append(Rows.row2("str", 1.23.toFloat))

      lazy val rs = x.resultSet

      new CompositeHandler().withQueryHandler(new QueryHandler {
        def apply(s: String, p: java.util.List[Parameter]) = rs
      }).whenSQLQuery("SELECT *", NO_PARAMS) aka "resultset" mustEqual rs
    }
  }
}