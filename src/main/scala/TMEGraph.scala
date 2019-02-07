import org.apache.spark.rdd.RDD

import scala.collection.mutable

class PageRank {

  import org.apache.spark.sql.SparkSession
  import org.apache.spark.SparkConf
  import org.apache.spark.sql.functions._
  import org.apache.spark.sql.Dataset
  import scala.reflect.ClassTag
  import org.apache.spark.sql.{Row, SparkSession}
  import org.apache.spark.{SparkConf, SparkContext}
  import org.apache.spark.SparkContext._

  import scala.collection.mutable._

  val conf = new SparkConf().setAppName("TME").setMaster("local[*]")
  conf.set("spark.driver.memory", "8G")

  val spark = SparkSession.builder.config(conf).getOrCreate()
  val sc = spark.sparkContext

  sc.setLogLevel("ERROR")
  val path = "/tmp/BDLE/dataset/"
  val file = "facebook_edges.csv"
  val lines = sc.textFile(path + file, 4)
  val links = lines.map { s =>
    val parts = s.split(",")
    (parts(0), parts(1))
  }.distinct().groupByKey().cache()
  var ranks = links.mapValues(v => 1.0)
  var iters = 5

  for (i <- 2 to iters) {
    val contribs = links.join(ranks).values.flatMap { case (urls, rank) =>
      val size = urls.size
      urls.map(url => (url, rank / size))
    }
    ranks = contribs.reduceByKey(_ + _).mapValues(0.15 + 0.85 * _)
  }

  val output5 = ranks

  //output5.collect().foreach(tup => println(tup._1 + " has rank: " + tup._2 + "."))

  output5.sortBy(x => -x._2).take(10).foreach(tup => println(tup._1 + " has rank: " + tup._2 + "."))

  //ou

  output5.top(10).foreach(tup => println(tup._1 + " has rank: " + tup._2 + "."))

  /*def iterate(iters: Int, initRanks : RDD[(String, Double)]): RDD[(String, Double)] = {
    var ranks = initRanks
    for (i <- 2 to iters) {
      val contribs = links.join(ranks).values.flatMap { case (urls, rank) =>
        val size = urls.size
        urls.map(url => (url, rank / size))
      }
      ranks = contribs.reduceByKey(_ + _).mapValues(0.15 + 0.85 * _)
    }
    ranks
  }*/

  ranks = links.mapValues(v => 1.0)
  iters = 10

  for (i <- 2 to iters) {
    val contribs = links.join(ranks).values.flatMap { case (urls, rank) =>
      val size = urls.size
      urls.map(url => (url, rank / size))
    }
    ranks = contribs.reduceByKey(_ + _).mapValues(0.15 + 0.85 * _)
  }

  val output10 = ranks

  //output5.collect().foreach(tup => println(tup._1 + " has rank: " + tup._2 + "."))

  output10.sortBy(x => -x._2).take(10).foreach(tup => println(tup._1 + " has rank: " + tup._2 + "."))

  //val rafinner = output5.join(output10).mapValues(v => (v._1 - v._2).abs)

  val rafinner = output5.join(output10).mapValues{case (x,y) => (x - y).abs}

  rafinner.sortBy(x => -x._2).take(10).foreach(tup => println(tup._1 + " has rank: " + tup._2 + "."))

  var prev = sc.accumulator(0.0)
  var crt = sc.accumulator(0.0)
  var maxIter = 100

  var nbNode = links.flatMap { case (k, v) => Seq(k) ++ v }.distinct().count()

  var err = 1e-3 * nbNode


  ranks = links.mapValues(v => 1.0)

  //Initialiser crt à la somme totale des scores des noeuds
  ranks.foreach(x=>crt+=x._2)

  var iter=0
  while(Math.abs(crt.value-prev.value)>err && iter < maxIter){
    //Calculer les nouveaux scores et les nouvelles valeurs de crt et prev
    prev.setValue(crt.value)

    val contribs = links.join(ranks).values.flatMap { case (urls, rank) =>
      val size = urls.size
      urls.map(url => (url, rank / size))
    }
    ranks = contribs.reduceByKey(_ + _).mapValues(0.15 + 0.85 * _)

    crt.setValue(0.0)
    ranks.foreach(x=>crt+=x._2)

    iter+=1
  }
  println("Terminé à l'itération "+iter)
  //Afficher les 5 premiers noeuds les mieux classés
  ranks.sortBy(x => -x._2).take(5).foreach(tup => println(tup._1 + " has rank: " + tup._2 + "."))

  val reverselinks = lines.map { s =>
    val parts = s.split(",")
    (parts(1), parts(0))
  }.distinct().groupByKey().cache()

  var rankHub = links.mapValues(v => 1.0)
  var rankAut = links.mapValues(v => 1.0)
  var i = 0

  while(i < iters){
    val contribsAut = links.join(rankHub).values.flatMap { case (urls, rank) =>
      urls.map(url => (url, rank))
    }
    rankAut = contribsAut.reduceByKey(_ + _)
    val autDist = rankAut.values.map(x => x*x).reduce(_ + _)
    rankAut = rankAut.mapValues(x => Math.sqrt(x / autDist))

    val contribsHub = reverselinks.join(rankAut).values.flatMap { case (urls, rank) =>
      urls.map(url => (url, rank))
    }
    rankHub = contribsHub.reduceByKey(_ + _)
    val hubDist = rankHub.values.map(x => x*x).reduce(_ + _)
    rankHub = rankHub.mapValues(x => Math.sqrt(x / hubDist))

    i += 1
  }

  rankHub.sortBy(x => -x._2).take(5).foreach(tup => println(tup._1 + " has rank: " + tup._2 + "."))
  rankAut.sortBy(x => -x._2).take(5).foreach(tup => println(tup._1 + " has rank: " + tup._2 + "."))


  prev = sc.accumulator(0.0)
  crt = sc.accumulator(0.0)
  maxIter = 100

  nbNode = links.flatMap{case (k, v) => Seq(k)++v}.distinct().count()

  err = 1e-3 * nbNode


  ranks = links.mapValues(v => 1.0)

  //Initialiser crt à la somme totale des scores des noeuds
  ranks.foreach(x=>crt+=x._2)

  iter = 0
  while(Math.abs(crt.value-prev.value) > err && iter < maxIter){
    //Calculer les nouveaux scores et les nouvelles valeurs de crt et prev
    prev.setValue(crt.value)

    val contribs = links.join(ranks).values.flatMap { case (urls, rank) =>
      val size = urls.size
      urls.map(url => (url, rank / size))
    }
    ranks = contribs.reduceByKey(_ + _).mapValues(0.15 + 0.85 * _)

    crt.setValue(0.0)
    ranks.foreach(x=>crt+=x._2)

    iter+=1
  }
  println("Terminé à l'itération "+iter)
  //Afficher les 5 premiers noeuds les mieux classés
  ranks.sortBy(x => -x._2).take(5).foreach(tup => println(tup._1 + " has rank: " + tup._2 + "."))

}