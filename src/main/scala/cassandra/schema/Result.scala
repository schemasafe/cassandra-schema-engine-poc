package cassandra.schema

case class Result(ksName: String, output: String Map String, outputLimit: Option[Int], input: Seq[(Option[Any], String)])

