package cassandra.schema

import java.lang.management.ManagementFactory
import javax.management.MBeanServer

import org.apache.cassandra.config.{Config, Schema => CassandraSchema}
import org.apache.cassandra.cql3.QueryProcessor
import org.apache.cassandra.cql3.statements._
import org.apache.cassandra.exceptions.RequestValidationException
import org.apache.cassandra.schema._
import org.apache.cassandra.service.ClientState

import scala.collection.JavaConverters._

object SchemaValidation {
  Config.setClientMode(true)

  val state = ClientState.forInternalCalls()

  def createSchema(schemaDefinitionStatements: Seq[String]): Either[String, Schema] = {

    //unregistered from cassandra instances to avoid javax.management.InstanceAlreadyExistsException
    val mbs: MBeanServer = ManagementFactory.getPlatformMBeanServer
    val instances = mbs.queryMBeans(null, null)
    instances.asScala
      .filter(
        _.getObjectName.getCanonicalName.contains("org.apache.cassandra"))
      .foreach(i => mbs.unregisterMBean(i.getObjectName))

    try {
      val data =
        schemaDefinitionStatements.foldLeft(Map.empty[String, Seq[Table]]) {
          (data, query) =>
            val parsed: CFStatement =
              QueryProcessor.parseStatement(query).asInstanceOf[CFStatement]
            parsed match {
              case keyspaceStatement: CreateKeyspaceStatement =>
                val ksName = keyspaceStatement.keyspace()
                val meta = KeyspaceMetadata.create(ksName, KeyspaceParams.local())
                CassandraSchema.instance.load(meta) //set new keyspace metadata
                data.updated(ksName, Seq.empty[Table]) //add keyspace in our local schema

              case tableStatement: CreateTableStatement.RawStatement =>
                tableStatement.prepare().statement.validate(state) //verify if the keyspace is already exist

                val statement: CreateTableStatement =
                  parsed
                    .asInstanceOf[CreateTableStatement.RawStatement]
                    .prepare(Types.none)
                    .statement
                    .asInstanceOf[CreateTableStatement]

                val ksName = tableStatement.keyspace()
                val meta = KeyspaceMetadata.create(ksName, KeyspaceParams.local(), Tables.of(statement.getCFMetaData))

                CassandraSchema.instance.unload(statement.getCFMetaData) // avoid to reload the same CFMetadata
                CassandraSchema.instance.load(meta) //set keyspaceMetadata with tables and views

                val columns =
                  statement.getCFMetaData.allColumns().asScala.map { column =>
                    column.name.toString -> column.`type`.asCQL3Type().toString
                  }
                val tables = data(ksName) ++ Seq(Table(name = statement.columnFamily(), columns.toMap))
                data.updated(ksName, tables) //set rows
            }
        }
      Right(Schema(data))
    } catch {
      case e: RequestValidationException => Left(e.getMessage)
    }
  }

  def checkSchema(schema: Schema, query: String): Either[String, Result] = {
    try {
      val parse = QueryProcessor.parseStatement(query)

      parse match {
        case select: SelectStatement.RawStatement =>
          val ksName = select.keyspace()
          val tableName = select.columnFamily()
          val schemaColumns = schema.data
            .get(ksName)
            .flatMap(tables => tables.find(_.name == tableName))
            .fold(Seq.empty[String]) { table =>
              table.columns.keySet.toSeq
            }

          val selectableColumns = {
            val columns =
              select.selectClause.asScala.map(_.selectable.toString)
            if (columns.isEmpty)
              schemaColumns //in case of SELECT *
            else columns
          }

          val isFailed = schema.data.get(ksName).isEmpty /*Keyspace does not exist*/ ||
            schemaColumns.isEmpty /*table does not exist*/ ||
            selectableColumns.intersect(schemaColumns).size < selectableColumns.size //undefined column name

          if (isFailed)
            select.prepare().statement.validate(state) //in SELECT if the schema is valid we will get an exception to be fixed in 3.11

          //NOW WE ARE SURE THAT THE QUERY IS VALID
          val output = {
            selectableColumns.map { name =>
              val columnType = schema.data(ksName).find(_.name == tableName).map(_.columns(name)).orNull
              name -> columnType
            }
          }.toMap

          val maybeLimit = if (select.limit == null) None else Some(select.limit.getText.toInt)

          Right(Result(ksName, output, maybeLimit, input = Seq.empty))

        case parsedStatement =>
          val preparedStatemet = parsedStatement.prepare()
          val modificationStatement = preparedStatemet.statement.asInstanceOf[ModificationStatement] //validate statement

          val ksName = modificationStatement.keyspace()
          val tableName = modificationStatement.columnFamily()

          val output = {
            val columns = modificationStatement.allOperations().asScala.map(_.column.toString)
            columns.map { name =>
              val columnType = schema.data(ksName).find(_.name == tableName).map(_.columns(name)).orNull
              name -> columnType
            }
          }.toMap

          val bounds = preparedStatemet.boundNames.asScala.map(_.name.toString)
          val input = output.map { case(name, columnType) =>
            val maybeValue = bounds.find(_ == name).fold[Option[String]](Some("TODO"))(_ => None) //todo get the updatedValues from modificationStatement
            maybeValue -> columnType
          }.toSeq

          Right(Result(ksName, output, None, input = input))
      }
    } catch {
      case e: RequestValidationException => Left(e.getMessage)
    }
  }
}
