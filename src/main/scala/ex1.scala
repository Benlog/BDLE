class ex1(){
  import org.apache.spark.sql.SparkSession
  import org.apache.spark.{SparkConf, SparkContext}

  val conf = new SparkConf().setAppName("TME").setMaster("local[*]")

  val sc = new SparkContext(conf)
  sc.setLogLevel("ERROR")

  val spark = SparkSession.builder.config(conf).getOrCreate()
  import spark.implicits._

  val data = sc.textFile("./data/wordcount.txt")

  val list = data.map(_.split(" "))

  list.take(5)

  list.take(100).map(_(3))

  val tuplelist = list.map(x => (x(0), x(2).toInt))

  tuplelist.take(5)

  tuplelist.reduceByKey(_+_).take(5)

  val tuplelist2 = list.map(x => (x(0).split("\\.")(0), x(2).toInt))

  tuplelist2.take(5)

  tuplelist2.reduceByKey(_+_).take(5)
}