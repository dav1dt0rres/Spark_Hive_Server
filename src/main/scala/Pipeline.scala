import java.io.File
import java.sql.{DriverManager, Statement}

import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode, SparkSession}

class Pipeline (var port: Int) {

  val warehouseLocation = new File("spark-warehouse").getAbsolutePath
  val con = DriverManager.getConnection(s"jdbc:hive2://localhost:$port/default", "", "")
  val stmt: Statement = con.createStatement

  import org.apache.spark.SparkConf
  import org.apache.spark.SparkContext
  import org.apache.spark.sql.hive.HiveContext

  val conf = new SparkConf().setMaster("local[*]").setAppName("Hive")
  val sc = new SparkContext(conf)
  val hiveContext = new HiveContext(sc)
  hiveContext.setConf("hive.metastore.uris", s"jdbc:hive2://localhost:$port/default")



  val Math_List: List[String] = List("C03 Math December 2019");

  val English_List= List("C03 December 2019 English")



  //var df=spark.read.format("com.databricks.spark.csv").option("header","true").option("inferSchema","true").load("C:\\Users\\david\\IdeaProjects\\myapp_spark\\src\\main\\resources\\blank.csv");



    def saveToDisk(dataframe: DataFrame, path:String): Unit ={
      dataframe.write.mode(SaveMode.Overwrite).parquet("C:\\Users\\david\\IdeaProjects\\spark-scala-boilerplate-master\\spark-warehouse\\" + path);
    }
}
