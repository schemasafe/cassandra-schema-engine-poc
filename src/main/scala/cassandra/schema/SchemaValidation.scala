package cassandra.schema

import java.lang.management.ManagementFactory
import java.util.Collections
import javax.management.MBeanServer

import org.apache.cassandra.config.{Config, Schema}
import org.apache.cassandra.cql3.statements.{CFStatement, CreateKeyspaceStatement, CreateTableStatement}
import org.apache.cassandra.cql3.{QueryOptions, QueryProcessor}
import org.apache.cassandra.exceptions.RequestValidationException
import org.apache.cassandra.schema._
import org.apache.cassandra.service.{ClientState, QueryState}

import scala.collection.JavaConverters._

object SchemaValidation {
  Config.setClientMode(true)

  val state = ClientState.forInternalCalls()
  val queryState = new QueryState(state)

  implicit class ValidateQuery(query: String) {
    def validateSyntax: Either[String, String] = {
      try {
        QueryProcessor.parseStatement(query)
        Right(query)
      } catch {
        case e: RequestValidationException => Left(e.getMessage)
      }
    }
  }

  def checkSchema(query: String): Either[String, String] = {
    query.validateSyntax //FIXME: validate statement
  }

  def createSchema(queries: Seq[String]): Either[String, String] = {

    //unregistered from cassandra instances to avoid javax.management.InstanceAlreadyExistsException
    val mbs: MBeanServer = ManagementFactory.getPlatformMBeanServer
    val instances = mbs.queryMBeans(null, null)
    instances.asScala
      .filter(
        _.getObjectName.getCanonicalName.contains("org.apache.cassandra"))
      .foreach(i => mbs.unregisterMBean(i.getObjectName))

    val options = QueryOptions.forInternalCalls(Collections.emptyList())

    try {
      queries.map { query =>
        val parsed: CFStatement =
          QueryProcessor.parseStatement(query).asInstanceOf[CFStatement]
        parsed match {
          case keyspaceStatement: CreateKeyspaceStatement =>
            val ksName = keyspaceStatement.keyspace()
            val meta = KeyspaceMetadata.create(ksName, KeyspaceParams.local())
            state.setKeyspace(ksName)
            Schema.instance.load(meta) //set new keyspace metadata

          case tableStatement: CreateTableStatement.RawStatement =>

            tableStatement.prepare().statement.validate(state) //verify if the keyspace is already exist

            val statement: CreateTableStatement =
              parsed
                .asInstanceOf[CreateTableStatement.RawStatement]
                .prepare(Types.none)
                .statement
                .asInstanceOf[CreateTableStatement]

            val meta =
              KeyspaceMetadata.create(tableStatement.keyspace(),
                KeyspaceParams.local(),
                Tables.of(statement.getCFMetaData))
            Schema.instance.load(meta) //set keyspaceMetadata with the columnfamily

          //Keyspace.openWithoutSSTables(state.getKeyspace) /* create keyspace instance with columnFamily and load keyspace//TO BE FIXED// upgrade cassandra version to 3.11*/
        }
      }
      Right("good")
    } catch {
      case e: RequestValidationException => Left(e.getMessage)
    }
  }
}
