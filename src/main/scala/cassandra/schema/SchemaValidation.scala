package cassandra.schema

import scala.collection.JavaConverters._

import java.lang.management.ManagementFactory
import java.util.Collections
import javax.management.MBeanServer

import org.apache.cassandra.config.Config
import org.apache.cassandra.cql3.statements.CreateKeyspaceStatement
import org.apache.cassandra.cql3.{QueryOptions, QueryProcessor}
import org.apache.cassandra.exceptions.RequestValidationException
import org.apache.cassandra.service.{ClientState, QueryState}

object SchemaValidation {
  Config.setClientMode(true)
  val state = ClientState.forInternalCalls()
  val queryState = new QueryState(state)

  implicit class ValidateQuery(query: String) {
    def validateQuery: Either[String, String] = {
      try {
        QueryProcessor.parseStatement(query, queryState)
        Right(query)
      } catch {
        case e: RequestValidationException => Left(e.getMessage)
      }
    }
  }

  def createSchema(queries: Seq[String]): Either[String, String] = {

    //unregistered from cassandra instances to avoid javax.management.InstanceAlreadyExistsException
    val mbs: MBeanServer = ManagementFactory.getPlatformMBeanServer
    val instances = mbs.queryMBeans(null, null)
    instances.asScala
      .filter(
        _.getObjectName.getCanonicalName.contains("org.apache.cassandra"))
      .foreach(i => mbs.unregisterMBean(i.getObjectName))

    val createKsQuery = queries.head
    val createTableQuery = queries.last
    createKsQuery.validateQuery.fold(
      error => Left(error),
      ksQuery => {
        val createKsStatement = QueryProcessor
          .parseStatement(ksQuery)
          .asInstanceOf[CreateKeyspaceStatement]
        val ksName = createKsStatement.keyspace()

        //create keyspace
        schemaChange(ksQuery, ksName)
        createTableQuery.validateQuery.fold(
          error => Left(error),
          tableQuery => {
            //create table
//            val createTable = QueryProcessor
//              .parseStatement(tableQuery)
//              .asInstanceOf[CreateTableStatement.RawStatement]
//            val createTableStatement = createTable
//              .prepare()
//              .statement
//              .asInstanceOf[CreateTableStatement]
            schemaChange(tableQuery, ksName)
            Right(createTableQuery)
          }
        )
      }
    )

  }

  private def schemaChange(query: String, ksName: String) = {
    try {
      state.setKeyspace(ksName)
      val prepared = QueryProcessor.parseStatement(query, queryState)
      val options = QueryOptions.forInternalCalls(Collections.emptyList())
      prepared.statement.execute(queryState, options)
    } catch {
      case e: RequestValidationException => println(e.getMessage)
    }
  }
}
