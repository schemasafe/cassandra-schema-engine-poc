package cassandra.schema

import SchemaValidation._
object Example extends App {

  val queries = Seq(
    "create keyspace if not exists test WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' :  1};",
    "create table test.posts(id int primary key, name text)")

  val result = SchemaValidation.createSchema(queries)
  val select = SchemaValidation.checkSchema("select * from test.posts")

  println(s"* create schema: $result")
  println(s"* check schema syntax: $select")
}
