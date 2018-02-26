package cassandra.schema

import org.apache.cassandra.config.{Config, DatabaseDescriptor, Schema}
import org.apache.cassandra.cql3.QueryProcessor
import org.apache.cassandra.cql3.statements.{
  CreateKeyspaceStatement,
  CreateTableStatement
}
import org.apache.cassandra.db.Keyspace
import org.apache.cassandra.exceptions.RequestValidationException
import org.apache.cassandra.schema.{KeyspaceMetadata, KeyspaceParams}
import org.apache.cassandra.service.{ClientState, QueryState}

object SchemaValidation {

  implicit class ValidateQuery(query: String) {
    val state = ClientState.forInternalCalls()

    def validateSyntax: Either[String, String] = {
      try {
        QueryProcessor.parseStatement(query)
        Right(query)
      } catch {
        case e: RequestValidationException => Left(e.getMessage)
      }
    }
    def validateQuery: Either[String, String] = {
      try {
        val queryState = new QueryState(state)
        QueryProcessor.parseStatement(query, queryState)
        Right(query)
      } catch {
        case e: RequestValidationException => Left(e.getMessage)
      }
    }
  }

  def createSchema(queries: Seq[String]): Either[String, String] = {
    Config.setClientMode(true)
    DatabaseDescriptor.forceStaticInitialization()
    Keyspace.setInitialized()
    val createKsQuery = queries.head
    val createTableQuery = queries.last
    createKsQuery.validateQuery.fold(
      error => Left(error),
      ksQuery => {
        val createKsStatement = QueryProcessor
          .parseStatement(ksQuery)
          .asInstanceOf[CreateKeyspaceStatement]
        val ksName = createKsStatement.keyspace()
        val keyspaceMetadata =
          KeyspaceMetadata.create(ksName, KeyspaceParams.simple(1))
        Schema.instance.addKeyspace(keyspaceMetadata)

        createTableQuery.validateSyntax.fold( //TODO validateQuery instead.
          error => Left(error),
          tableQuery => {
            val createTable = QueryProcessor
              .parseStatement(tableQuery)
              .asInstanceOf[CreateTableStatement.RawStatement]
//            val createTableStatement = createTable
//              .prepare()
//              .statement
//              .asInstanceOf[CreateTableStatement]
//            val tables = createTableStatement.getCFMetaData
//            Schema.instance.addTable(tables)
//             FIXME: There is an exception javax.management.InstanceAlreadyExistsException: org.apache.cassandra.db:type=StorageService

            Right(createTableQuery)
          }
        )
      }
    )

  }

}
