package cassandra.schema

object Example extends App {

  val queries = Seq(
    "create keyspace test WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' :  1};",
    "create table test.zz (y int primary key)")

  val result = SchemaValidation.createSchema(queries)
  println(s"* result: $result")
}
