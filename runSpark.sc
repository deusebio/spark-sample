
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

val conf = new SparkConf().setAppName("spark-test").setMaster("local")

implicit val sc = new SparkContext(conf)

implicit val sqlContext = new SQLContext(sc)

