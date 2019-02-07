
class S1() {

  //début du script
  import org.apache.spark.sql.SparkSession
  import org.apache.spark.{SparkConf, SparkContext}

  val conf = new SparkConf().setAppName("TME").setMaster("local[*]")

  val sc = new SparkContext(conf)
  sc.setLogLevel("ERROR")

  val spark = SparkSession.builder.config(conf).getOrCreate()
  import spark.implicits._

  // Exemple 1

  val t = List(("a",10),("b",20),("d",25)).toDF("Nom","Age")
  t.show(2)

  t.createOrReplaceTempView("Etu")

  val r1= spark.sql("""
    SELECT Nom, Age
    FROM Etu
    WHERE Age>18
    """)
  r1.show()

  // compléter ici le script ....



} // fin de la classe