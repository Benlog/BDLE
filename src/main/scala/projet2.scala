import scala.collection.JavaConverters._

//Une classe contenant le script
class projet2() {

  //début du script
  import org.apache.spark.SparkConf
  import org.apache.spark.sql.{SparkSession, _}

  val conf = new SparkConf().setAppName("TME").setMaster("local[*]")

  val spark = SparkSession.builder.config(conf).getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  import org.apache.spark.sql.functions._
  import spark.implicits._

  /*Exemple de lecture d'un fichier JSON */

  val doc = spark.read.json("/users/Etu3/3200403/BDLE/arxiv_2018-11-07/arxiv_documents_2018-11-07.json")
  val top = spark.read.json("/users/Etu3/3200403/BDLE/arxiv_2018-11-07/arxiv_topic_term_2018-11-07.json")
  val voc = spark.read.json("/users/Etu3/3200403/BDLE/arxiv_2018-11-07/arxiv_vocab_2018-11-07.json")
  doc.printSchema()
  top.printSchema()
  voc.printSchema()
  //val d1 = doc.select("title", "categories")
  //d1.show(10,false)

  top.join(voc, $"termId" === $"id").drop("termId", "id").show(5)
  top.map { case Row(p: String, te: String, to: Long, w: Double) => (to, te, w, p) }.show(5)


  top.groupByKey { case Row(p: String, te: String, to: Long, w: Double) => to }


  val topics = top.select("topicId", "period").distinct().sort("topicId", "period")
  val crosstopics = topics.crossJoin(topics.withColumnRenamed("topicId", "topicId2").withColumnRenamed("period", "period2"))

  //For Cosine Similarity

  val joinA = crosstopics.join(top.withColumnRenamed("topicId", "topicId3")
    .withColumnRenamed("period", "period3"), $"topicId" === $"topicId3" && $"period" === $"period3").drop("topicId3", "period3")
  val joinB = joinA.join(top.withColumnRenamed("termId", "termId2")
    .withColumnRenamed("weight", "weight2").withColumnRenamed("topicId", "topicId3")
    .withColumnRenamed("period", "period3"), $"topicId2" === $"topicId3" && $"period2" === $"period3").drop("topicId3", "period3")

  val sameWeight = joinB.where($"termId" === $"termId2").drop("termId", "termId2")

  //Scalar product
  val prod_C = sameWeight.map { case Row(t1: Long, p1: String, t2: Long, p2: String, w1: Double, w2: Double) => (t1, p1, t2, p2, w1 * w2, w1 * w1, w2 * w2) }
  val scal_C = prod_C.groupBy("_1", "_2", "_3", "_4").sum("_5", "_6", "_7")

  val matrix_C = scal_C.map { case Row(a: Long, b: String, c: Long, d: String, x: Double, y: Double, z: Double) => (a, b, c, d, x / Math.sqrt(y * z)) }

  val moy = matrix_C.select(avg("_5"))

  //For KL Distance -> Abandonned

  //val probas_KL = sameWeight.map{case Row(t1 : Long, p1 : String, t2 : Long, p2 : String, w1 : Double, w2 : Double) => (t1, p1, t2, p2, w1 * Math.log(w1/w2))}
  //val matrix_KL = probas_KL.groupBy("_1", "_2", "_3", "_4").sum("_5")

  //For Bhattacharyya's similarity


  val top_sum = top.groupBy($"period", $"topicId").sum("weight").withColumnRenamed("topicId", "topicId2").withColumnRenamed("period", "period2")
  val top_probs = top.join(top_sum, $"period" === $"period2" && $"topicId" === $"topicId2").drop("topicId2", "period2")
  val top_probas = top_probs.map { case Row(p: String, t: String, to: Long, w: Double, s: Double) => (p, t, to, w / s) }.withColumnRenamed("_1", "period").withColumnRenamed("_2", "termId").withColumnRenamed("_3", "topicId").withColumnRenamed("_4", "weight")

  val joinA_B = crosstopics.join(top_probas.withColumnRenamed("topicId", "topicId3")
    .withColumnRenamed("period", "period3"), $"topicId" === $"topicId3" && $"period" === $"period3").drop("topicId3", "period3")
  val joinB_B = joinA_B.join(top_probas.withColumnRenamed("termId", "termId2")
    .withColumnRenamed("weight", "weight2").withColumnRenamed("topicId", "topicId3")
    .withColumnRenamed("period", "period3"), $"topicId2" === $"topicId3" && $"period2" === $"period3").drop("topicId3", "period3")

  val sameWeight_B = joinB_B.where($"termId" === $"termId2").drop("termId", "termId2")

  val probas_B = sameWeight_B.map { case Row(t1: Long, p1: String, t2: Long, p2: String, w1: Double, w2: Double) => (t1, p1, t2, p2, Math.sqrt(w1 * w2)) }

  val matrix_B = probas_B.groupBy("_1", "_2", "_3", "_4").sum("_5")


  // Filtrage

  //val filteredB = matrix_B.filter($"sum(_5)" >= 0.9)

  //Matrice du graphe des topics ayant la plus grande similarité avec le topic de l'année suivante => Trouver des patterns d'évolution

  val evo_tmp = matrix_B.filter($"_2" + 1 === $"_4")
  val evoB = evo_tmp.groupBy("_1", "_2").max("sum(_5)").withColumnRenamed("max(sum(_5))", "maxsum")
  val evoF = evoB.join(evo_tmp.withColumnRenamed("_1", "_11")
    .withColumnRenamed("_2", "_22")
    .withColumnRenamed("sum(_5)", "sum"), $"_1" === $"_11" && $"_2" === $"_22" && $"maxsum" === $"sum").drop("_11", "_22", "maxsum").dropDuplicates("_1", "_2")


  val evoC_tmp = matrix_C.filter($"_2" + 1 === $"_4")
  val evoC = evoC_tmp.groupBy("_1", "_2").max("_5").withColumnRenamed("max(_5)", "max")
  val evoFC = evoC.join(evoC_tmp.withColumnRenamed("_1", "_11")
    .withColumnRenamed("_2", "_22")
    , $"_1" === $"_11" && $"_2" === $"_22" && $"max" === $"_5").drop("_11", "_22", "max").dropDuplicates("_1", "_2")

  matrix_B.summary().show()


  //Composantes connexes

  //Remplace les indices double termId, Année par un id simple
  val topicsId = topics.withColumn("id", monotonically_increasing_id)

  val evofSimp = evoF.join(topicsId.withColumnRenamed("id", "id1"), $"_1" === $"topicId" && $"_2" === $"period").drop("_1", "_2", "topicId", "period").join(topicsId.withColumnRenamed("id", "id2"), $"_3" === $"topicId" && $"_4" === $"period").drop("_3", "_4", "topicId", "period")

  //Ca permet d'obtenir les noeuds en commun sur les deux graphes a et b
  def interList(a: List[(Long, Long)], b: List[(Long, Long)]): List[Long] = {
    a.flatMap { case (x, y) => List(x, y) }.intersect(b.flatMap { case (x, y) => List(x, y) })
  }

  //Ca fusionne tous les éléments de a et b qui ont des noeuds en commun
  def fusGraph(a: List[(Long, Long)], b : List[List[(Long, Long)]]) : List[(Long, Long)] = {
    if(b==List()) a
    var h::t = b
    while(interList(a,h) == List()) {
      if (t == List()) a
      h = t.head
      t = t.tail
    }
    fusGraph(a++h, b diff h)
    }



  def extract_comp(a: List[List[(Long, Long)]], b: List[List[(Long, Long)]]): List[List[(Long, Long)]] = {
    val tmp = a ++ b
    if(tmp == List()) List()
    var h = tmp.head
    var t = tmp.tail
    var r = List(fusGraph(h, t))
    while(t != List()) {
      h = t.head
      t = t.tail
      r = fusGraph(h, t)::r
    }
    r
  }

  val subGraph = evofSimp.map{ case Row(x : Double, y : Long, z : Long) => List(List((y,z)))}

  val connex = subGraph.reduce((x,y) => (extract_comp(x,y)))

}// fin de la classe