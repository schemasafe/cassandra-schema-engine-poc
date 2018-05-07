package cassandra.schema

import java.lang.management.ManagementFactory
import javax.management.MBeanServer
import scala.collection.JavaConverters._

import org.apache.cassandra.config.{CFMetaData, DatabaseDescriptor, Schema => CassandraSchema}
import org.apache.cassandra.cql3.QueryProcessor
import org.apache.cassandra.cql3.statements._
import org.apache.cassandra.exceptions.RequestValidationException
import org.apache.cassandra.schema._
import org.apache.cassandra.service.ClientState

object SchemaValidation {
  lazy val state: ClientState = ClientState.forInternalCalls()
  DatabaseDescriptor.clientInitialization()

  def createSchema(schemaDefinitionStatements: Seq[String]): Either[String, Schema] = {

    //unregistered from cassandra instances to avoid javax.management.InstanceAlreadyExistsException
    val mbs: MBeanServer = ManagementFactory.getPlatformMBeanServer
    val instances = mbs.queryMBeans(null, null)
    instances.asScala
      .filter(
        _.getObjectName.getCanonicalName.contains("org.apache.cassandra"))
      .foreach(i => mbs.unregisterMBean(i.getObjectName))

    try {
      val data = schemaDefinitionStatements.foldLeft(Map.empty[String, Seq[Table]]) {
          (data, query) =>
            val creationStatement =
              QueryProcessor.prepareInternal(query).statement

            creationStatement match {
              case keyspaceStatement: CreateKeyspaceStatement =>
                val ksName = keyspaceStatement.keyspace()
                addKeyspace(ksName)
                data.updated(ksName, Seq.empty[Table]) //add keyspace in our local schema

              case statement: CreateTableStatement =>
                val table = statement.getCFMetaData
                addTable(table)

                val ksName = statement.keyspace()
                val columns = table.allColumns().asScala.map { column =>
                  column.name.toString -> column.`type`.asCQL3Type().toString
                }

                val tables = data(ksName) ++ Seq(Table(name = statement.columnFamily(), columns.toMap))
                data.updated(ksName, tables) //add table with columns in keyspace
            }
        }
      Right(Schema(data))
    } catch {
      case e: RequestValidationException => Left(e.getMessage)
      case ex: Exception => Left(s"Internal error: ${ex.getMessage}")
    }
  }

  def checkSchema(schema: Schema, query: String): Either[String, Result] = {
    try {
      val prepared = QueryProcessor.prepareInternal(query)
      val statement = prepared.statement
      statement match {
        case _: SelectStatement => ??? //internal error
        case st: ModificationStatement =>
          val ksName = st.keyspace()
          val updatedColumns = st.updatedColumns().asScala
          val output = updatedColumns.map(col => col.name.toString -> col.`type`.asCQL3Type().toString).toMap
          val bounds = prepared.boundNames.asScala.map(_.name.toString).toList
          val input = output.map {
            case (name, columnType) =>
              val maybeValue = bounds.find(_ == name).fold[Option[String]](Some("TODO"))(_ => None)
              maybeValue -> columnType
          }.toSeq

          Right(Result(ksName, output, None, input = input))
      }
    } catch {
      case e: RequestValidationException => Left(e.getMessage)
      case ex: Exception => Left(s"Internal error: ${ex.getMessage}")

    }
  }

  private def addKeyspace(ksName: String): Unit = {
    val meta = KeyspaceMetadata.create(ksName, KeyspaceParams.local())
    CassandraSchema.instance.addKeyspace(meta)
  }

  private def addTable(cfMeta: CFMetaData): Unit = {
    //CassandraSchema.instance.addTable(cfMeta) //TODO store table in CassandraSchema instance but there is an exception https://github.com/apache/cassandra/pull/223 :( and then we can implement select
    val ks = CassandraSchema.instance.getKSMetaData(cfMeta.ksName)
    val s = ks.withSwapped(ks.tables.`with`(cfMeta))
    CassandraSchema.instance.setKeyspaceMetadata(s)
  }

}
