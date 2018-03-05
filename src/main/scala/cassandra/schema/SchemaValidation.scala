package cassandra.schema

import java.lang.management.ManagementFactory
import javax.management.MBeanServer

import org.apache.cassandra.config.{Config, Schema => CassandraSchema}
import org.apache.cassandra.cql3.QueryProcessor
import org.apache.cassandra.cql3.statements.{CFStatement, CreateKeyspaceStatement, CreateTableStatement}
import org.apache.cassandra.db.Keyspace
import org.apache.cassandra.exceptions.RequestValidationException
import org.apache.cassandra.schema._
import org.apache.cassandra.service.{ClientState, QueryState}

import scala.collection.JavaConverters._

object SchemaValidation {
  Config.setClientMode(true)

  val state = ClientState.forInternalCalls()
  val queryState = new QueryState(state)

  implicit class ValidateQuery(query: String) {
    def validateSyntax: Either[String, String] =
      try {
        QueryProcessor.parseStatement(query)
        Right(query)
      } catch {
        case e: RequestValidationException => Left(e.getMessage)
      }
  }

  def checkSchema(query: String): Either[String, String] = {
    query.validateSyntax //FIXME: validate statement after upgrating cassandra version 3.11
  }

  def createSchema(queries: Seq[String]): Either[String, Schema] = {

    //unregistered from cassandra instances to avoid javax.management.InstanceAlreadyExistsException
    val mbs: MBeanServer = ManagementFactory.getPlatformMBeanServer
    val instances = mbs.queryMBeans(null, null)
    instances.asScala
      .filter(
        _.getObjectName.getCanonicalName.contains("org.apache.cassandra"))
      .foreach(i => mbs.unregisterMBean(i.getObjectName))

    try {
      val data = queries.foldLeft(Map.empty[String, Seq[Table]]) { (data, query) =>
        val parsed: CFStatement =
          QueryProcessor.parseStatement(query).asInstanceOf[CFStatement]
        parsed match {
          case keyspaceStatement: CreateKeyspaceStatement =>
            val ksName = keyspaceStatement.keyspace()
            val meta = KeyspaceMetadata.create(ksName, KeyspaceParams.local())
            state.setKeyspace(ksName)
            CassandraSchema.instance.load(meta) //set new keyspace metadata
            Keyspace.openWithoutSSTables(ksName) //FIXME to be deleted after upgrading cassandra 3.11
            data.updated(state.getKeyspace, Seq.empty[Table]) //add keyspace in our local schema

          case tableStatement: CreateTableStatement.RawStatement =>

            tableStatement.prepare().statement.validate(state) //verify if the keyspace is already exist

            val statement: CreateTableStatement =
              parsed
                .asInstanceOf[CreateTableStatement.RawStatement]
                .prepare(Types.none)
                .statement
                .asInstanceOf[CreateTableStatement]

            val ksName = tableStatement.keyspace()
            val meta =
              KeyspaceMetadata.create(ksName,
                KeyspaceParams.local(),
                Tables.of(statement.getCFMetaData))

            CassandraSchema.instance.unload(statement.getCFMetaData) // avoid to reload the same CFMetadata
            CassandraSchema.instance.load(meta) //set keyspaceMetadata with tables and views
            CassandraSchema.instance.getKeyspaceInstance(state.getKeyspace).setMetadata(meta) //FIXME to be deleted after upgrading cassandra 3.11

            val columns =
              statement.getCFMetaData.allColumns().asScala.map(column => Column(column.name.toString,
                column.`type`.asCQL3Type().toString))
            val tables = data(state.getKeyspace) ++ Seq(Table(name = statement.columnFamily(), columns.toSeq))
            data.updated(state.getKeyspace, tables) //set rows

          //Keyspace.openWithoutSSTables(state.getKeyspace) /* create keyspace instance with columnFamily and load keyspace//TO BE FIXED// upgrade cassandra version to 3.11*/
        }
      }
      Right(Schema(data))
    } catch {
      case e: RequestValidationException => Left(e.getMessage)
    }
  }
}
