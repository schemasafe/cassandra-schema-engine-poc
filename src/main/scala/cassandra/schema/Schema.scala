package cassandra.schema

case class Table(name: String, columns: String Map String)
case class Schema(data: String Map Seq[Table])
