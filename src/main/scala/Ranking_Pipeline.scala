import org.apache.spark.sql.{SQLContext, SparkSession}


class Ranking_Pipeline (port: Int) extends Pipeline (port)  {


 // import spark.implicits._
  val Name="Pivot Pipeline: Ranks the Student based off his past responses after every problem given a test (no history carries over) "
  val base_table_list: List[String] = List("English_Responses_Pivoted","Math_Responses_Pivoted");
  def run(){
    //df = spark.read.parquet("C:\\Users\\david\\IdeaProjects\\spark-scala-boilerplate-master\\spark-warehouse\\responses_table")
    //df.createOrReplaceTempView("responses_table")
  }

}
