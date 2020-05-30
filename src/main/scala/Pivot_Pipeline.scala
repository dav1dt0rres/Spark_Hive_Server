import org.apache.spark.sql.{SQLContext, SparkSession}

import java.sql.ResultSet
object Implicits {
  implicit class ResultSetStream(resultSet: ResultSet) {
    def toStream: Stream[ResultSet] = {
      new Iterator[ResultSet] {
        def hasNext = resultSet.next()
        def next() = resultSet
      }.toStream
    }
  }
}
class Pivot_Pipeline (port: Int) extends Pipeline (port)  {
  //import spark.implicits._
  val Name="Pivot Pipeline: Ranks the Student based off his past responses after every problem given a test (no history carries over) "
  val base_table_list: List[String] = List("responses_table");
  def run(){
    //PivotEnglish_hive();
    //PivotMath();
    //PivotReading();
    PivotEnglish();
    //PivotScience();
  }

  def PivotEnglish_hive(): Unit ={
    //stmt.execute("SELECT FirstName,LastName,StudentID,QuizName, stack(52 ,1, Mark1, 2, Mark2, 3,Mark3,4,Mark4,5,Mark5,6,Mark6,7,Mark7,8,Mark8,\" +\n      \"9,Mark9,10,Mark10,11,Mark11,12,Mark12,13,Mark13,14,Mark14,15,Mark15,16,Mark16,17,Mark17,18,Mark18,19,Mark19,20,Mark20,21,Mark21,22,Mark22,23,Mark23,24,Mark24,25,Mark25,26,Mark26,27,Mark27,28,Mark28,29,Mark29,\" +\n      \"30,Mark30,31,Mark31,32,Mark32,33,Mark33,34,Mark34,35,Mark35,36,Mark36,37,Mark37,38,Mark38,39,Mark39,40,Mark40,41,Mark41,42,Mark42,43,Mark43,44,Mark44,45,Mark45,46,Mark46,47,Mark47,48,Mark48,49,Mark49,\" +\n      \"50,Mark50,51,Mark51,52,Mark52) as (Number,Wrong) \" +\n      \"                         FROM responses_table")
    val resultSet = stmt.executeQuery("select * from emp");
    import Implicits._
    println("Querying response table");
    resultSet.toStream
      .foreach(result => println(s"result after fetching ids ${ result.getInt("FirstName") }"))
  }
  def PivotEnglish(): Unit ={


    //df = spark.read.parquet("C:\\Users\\david\\IdeaProjects\\spark-scala-boilerplate-master\\spark-warehouse\\responses_table")
    //df.createOrReplaceTempView("emp")



    val DF = hiveContext.read.parquet("C:\\Users\\david\\IdeaProjects\\spark-scala-boilerplate-master\\spark-warehouse\\responses_table")

    DF.createOrReplaceTempView("parquetFile")
    val English_df= hiveContext.sql("SELECT FirstName,LastName,StudentID,QuizName, stack(52 ,1, Mark1, 2, Mark2, 3,Mark3,4,Mark4,5,Mark5,6,Mark6,7,Mark7,8,Mark8," +
      "9,Mark9,10,Mark10,11,Mark11,12,Mark12,13,Mark13,14,Mark14,15,Mark15,16,Mark16,17,Mark17,18,Mark18,19,Mark19,20,Mark20,21,Mark21,22,Mark22,23,Mark23,24,Mark24,25,Mark25,26,Mark26,27,Mark27,28,Mark28,29,Mark29," +
      "30,Mark30,31,Mark31,32,Mark32,33,Mark33,34,Mark34,35,Mark35,36,Mark36,37,Mark37,38,Mark38,39,Mark39,40,Mark40,41,Mark41,42,Mark42,43,Mark43,44,Mark44,45,Mark45,46,Mark46,47,Mark47,48,Mark48,49,Mark49," +
      "50,Mark50,51,Mark51,52,Mark52) as (Number,Wrong) " +
      "                         FROM parquetFile");
    English_df.show();
      //English_df.write.partitionBy("QuizName","LastName","FirstName").parquet("C:\\Users\\david\\IdeaProjects\\spark-scala-boilerplate-master\\spark-warehouse\\English_Responses_Pivoted") //This writes it for the FIRST TIME. THIS MIGHT NEVER BE RECALLED AGAIN
      saveToDisk(English_df,"English_Responses_Pivoted")
  }
  def PivotMath(): Unit ={

  }
  def loadFromDisk(): Unit ={


  }
}
