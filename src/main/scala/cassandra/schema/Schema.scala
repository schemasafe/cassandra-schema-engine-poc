package cassandra.schema

case class Column(name: String, `type`: String)
case class Table(name: String, columns: Seq[Column])
case class Schema(data: String Map Seq[Table])
