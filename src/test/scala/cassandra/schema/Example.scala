package cassandra.schema

object Example extends App {

  val queries = Seq(
    "create keyspace if not exists test WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' :  1};",
    "create table test.posts(id int primary key, name text)")

  val result = SchemaValidation.createSchema(queries)
  val select = SchemaValidation.checkSchema(result.right.get, "select * from test.posts limit 1")
  val update = SchemaValidation.checkSchema(result.right.get, "update test.posts set name = ? where id = 1")
  val delete = SchemaValidation.checkSchema(result.right.get, "delete from test.posts where id = 1")
  val insert = SchemaValidation.checkSchema(result.right.get, "INSERT INTO test.posts (id, name) VALUES (?, ?) IF NOT EXISTS;")

  println(s"* create schema: $result")
  println(s"* check select statement: $select")
  println(s"* check update statement: $update")
  println(s"* check delete statement: $delete")
  println(s"* check insert statement: $insert")

}
