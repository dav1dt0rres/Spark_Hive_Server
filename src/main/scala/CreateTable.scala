import org.apache.spark.api.java.{JavaRDD, JavaSparkContext}
import org.apache.spark.sql.{SQLContext, SaveMode, SparkSession}
import java.io.File
 class Table (var spark: SparkSession)  {
  // private val sc =sparkContext;



   private var df=spark.read.
     format("com.databricks.spark.csv").
     option("header","true").
     option("inferSchema","true").
     load("C:\\Users\\david\\IdeaProjects\\myapp_spark\\src\\main\\resources\\blank.csv")


  def changeSchema(name: String): Unit = {
    // This constructor has one parameter, name.
  }
  def UnionAll(newtable : Table):Unit={
      println("inside Union All")
  }
  def printSchema(): Unit = {
    df.printSchema;
  }

  def writetoDisk = 0

  def loadData(path:String): Unit = {
      df=spark.read.
      format("com.databricks.spark.csv").
      option("header","true").
      option("inferSchema","true").
      load(path)
      println("Count")
      println(df.count())


  }
   def updateAllBaseTables(): Unit ={ //this takes the excel.csv files and updates the base parquet files
     val item_analysis_path="C:\\Users\\david\\IdeaProjects\\myapp_spark\\src\\main\\resources\\item_analysis.csv";
     val tag_analysis_path="C:\\Users\\david\\IdeaProjects\\myapp_spark\\src\\main\\resources\\tag_analysis.csv";
     val response_path="C:\\Users\\david\\IdeaProjects\\myapp_spark\\src\\main\\resources\\response_table.csv";
     var dm  = List[String](item_analysis_path,tag_analysis_path,response_path);
     dm.foreach {savetoDisk}

   }
   def showParquet(): Unit ={
     df = spark.read.parquet("C:\\Users\\david\\IdeaProjects\\spark-scala-boilerplate-master\\spark-warehouse\\item_analysis_table")
     df.createOrReplaceTempView("parquetFile")
     val namesDF = spark.sql("SELECT * FROM parquetFile");
     namesDF.show();
   }

   def loadfromDisk(path:String): Unit ={


     df = spark.read.parquet("C:\\Users\\david\\IdeaProjects\\spark-scala-boilerplate-master\\spark-warehouse\\item_analysis_table")
     df.createOrReplaceTempView("parquetFile")
     val namesDF = spark.sql("SELECT * FROM parquetFile");
     namesDF.show();


   }
   def findColumn(s: Seq[String]): Boolean ={
     //this should only be called when updating the base tables.
     //println("Column: "+s.head)
     if (s.isEmpty) {
       println("NEVER FOUDN A MATCHING COLUMN");
       return false;
     }
     if(s.head=="Stu1"){
       println("Updating responses_table");
       df.write.mode(SaveMode.Overwrite).parquet("C:\\Users\\david\\IdeaProjects\\spark-scala-boilerplate-master\\spark-warehouse\\responses_table")

       return true;
     }
     else if(s.head=="Tag"){
       println("Updating tag analysis table");
       df.write.mode(SaveMode.Overwrite).parquet("C:\\Users\\david\\IdeaProjects\\spark-scala-boilerplate-master\\spark-warehouse\\tag_analysis_table")

       return true;
     }
     else if(s.head=="P-Score"){
       println("Updating item analysis table");
       df.write.mode(SaveMode.Overwrite).parquet("C:\\Users\\david\\IdeaProjects\\spark-scala-boilerplate-master\\spark-warehouse\\item_analysis_table")


       return true;
     }
     else findColumn(s.tail)
   }
   def savetoDisk(path:String): Unit ={
     println("Saving to Disk: "+path)
     df=spark.read.
       format("com.databricks.spark.csv").
       option("header","true").
       option("inferSchema","true").
       load(path)
     val selectColumns = df.columns.toSeq
     return findColumn(selectColumns);


   }
}
