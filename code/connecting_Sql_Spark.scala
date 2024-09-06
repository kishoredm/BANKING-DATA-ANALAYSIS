import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.{SparkConf, SparkContext}



val conf = new SparkConf().setAppName("MySparkApplication").setMaster("local[2]")
val sc = new SparkContext(conf)

val spark = SparkSession.builder.appName("MySparkApplication").getOrCreate()

val spark = SparkSession.builder()
  .appName("JDBC Spark Example")
  .config("spark.master", "local")
  .getOrCreate()

// Database connection properties
val url = "jdbc:mysql://localhost:3306/demo"
val properties = new java.util.Properties()
properties.setProperty("user", "root")
properties.setProperty("password", "shyamsurya321#")

// Read data from the database table using JDBC
val data = spark.read.jdbc(url, "BankData", properties)
data.show()

val firstElement = data.first()