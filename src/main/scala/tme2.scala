class base(){

  import org.apache.spark.sql.SparkSession
  import org.apache.spark.{SparkConf, SparkContext}

  val conf = new SparkConf().setAppName("TME").setMaster("local[*]")

  val sc = new SparkContext(conf)
  sc.setLogLevel("ERROR")

  val spark = SparkSession.builder.config(conf).getOrCreate()
  import spark.implicits._

  case class Triple(sujet: String, prop: String, objet: String)

  val yagoFile = "/tmp/BDLE/dataset/yagoFacts5M.txt"

  val yago = sc.textFile(yagoFile).
    map(ligne => ligne.split("\\t")).coalesce(8).
    map(tab => Triple(tab(0), tab(1), tab(2))).toDS()

  yago.persist()
  yago.count()

  val r1 = yago.where("prop = '<hasCapital>' and objet = '<Nantes>'").
    withColumnRenamed("sujet","x").
    select("x")
  r1.show(5)

  val r2 = yago.where("sujet = '<Barack_Obama>'").
    withColumnRenamed("prop","p").
    withColumnRenamed("objet","o").
    select("p", "o")
  r2.show()

  val r3 = yago.where("prop = '<livesIn>' and objet = '<Paris>'").
    join(yago.where("prop = '<isLeaderOf>'")
      .withColumnRenamed("objet","z"), "sujet").
    withColumnRenamed("sujet","x").
    select("x", "z")
  r3.show()

  val r4 = yago.where("prop = '<playsFor>'").
  withColumnRenamed("sujet","x").
    withColumnRenamed("objet","y").
    join(yago.where("prop = '<isLocatedIn>' and objet = '<United_Kingdom>'").
      withColumnRenamed("sujet","y")
      , "y").
    select("y", "x")
  r4.show(5)


}