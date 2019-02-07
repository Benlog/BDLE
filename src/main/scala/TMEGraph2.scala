import sun.security.provider.certpath.Vertex

class TMEGraph2 {

  import org.apache.spark.SparkConf
  import org.apache.spark.sql.SparkSession
  import org.apache.spark.graphx._
  import org.apache.spark.rdd.RDD

  import scala.collection.mutable._

  val conf = new SparkConf().setAppName("TME").setMaster("local[*]")
  conf.set("spark.driver.memory", "8G")

  val spark = SparkSession.builder.config(conf).getOrCreate()
  val sc = spark.sparkContext

  sc.setLogLevel("ERROR")
  val path = "/tmp/BDLE/dataset/"
  val file = "facebook_edges_prop.csv"
  val file2 = "facebook_users_prop.csv"
  val inters = sc.textFile(path + file, 4)
  val users = sc.textFile(path + file2, 4)

  val vertexes : RDD[(VertexId, (String, String, Int))] = users.map{ x => val t = x.split(","); (t(0).toLong, (t(1), t(2), t(3).toInt))}.distinct()

  val edges : RDD[Edge[(String, Int)]] = inters.map{x => val t = x.split(","); Edge(t(0).toLong, t(1).toLong, (t(2), t(3).toInt))}.distinct()

  val graph = Graph(vertexes, edges)

  print(graph.vertices.filter{case x => x._2._1 == "Kendall" }.take(1)(0))

  graph.triplets.filter( x => x.srcAttr._1 == "Kendall" || x.dstAttr._1 == "Kendall").count()

  graph.triplets.filter(x =>
    (x.srcAttr._1 == "Kendall"  || x.dstAttr._1 == "Kendall") && x.attr._1 == "friend").map(t =>
    if(t.srcAttr._1=="Kendall") t.dstAttr._1 else t.srcAttr._1).collect().foreach(println(_))


  graph.aggregateMessages[(Int, Int)]({x => x.sendToDst(x.srcAttr._3, 1);
    x.sendToSrc(x.dstAttr._3, 1)}, {case (x, y) => (x._1 + y._1, x._2 + y._2)}).mapValues(x => (x._1 / x._2).toFloat).join(vertexes).values.take(5)
}
