import java.io.File

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.sql.execution.datasources.CreateTable
import org.apache.spark.sql.types._
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.api.java.JavaSparkContext
import java.sql.{DriverManager, SQLException, Statement}
import scala.util.Try
import hive.server.HiveEmbeddedServer2



import scala.util.Try
import org.datanucleus.store.rdbms.connectionpool.DatastoreDriverNotFoundException
object HelloClient {
  private val driverName: String = "org.apache.hive.jdbc.HiveDriver"
  @throws[SQLException]
  def main(args: Array[String]) {
    if (Try(Class.forName(driverName)).isFailure) {
      throw new DatastoreDriverNotFoundException("driver not found")
    }

    val port =10000

    val pipeline=new Pivot_Pipeline(port);
    pipeline.run();


  }
}
