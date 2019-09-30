import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import scala.math._
import scala.util.Sorting
import scala.io.Source
import scala.util.Random
import scala.collection.mutable.{ListBuffer}
import java.io._
import com.github.fommil.netlib.BLAS.{getInstance => blas}



object CosineLSH {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("cosine").setMaster("local[3]")
    val sc = new SparkContext(sparkConf)
    val path0=args(0)
    val csv = sc.textFile(args(0))
    val header = csv.first()
    val b = 10
    val r = 3
    val ratinguser = csv.filter(x => x != header).map(x => x.split(",")).map(x => (x(1).toInt, x(0).toInt)).distinct().groupByKey()
    val user = ratinguser.mapValues(_.toSet).collect.sortBy(_._1)
    val usersize = csv.filter(x => x != header).map(x => x.split(",")).map(x => (x(0))).distinct().count().toInt
    //println(usersize)

    val hashtable=hash(b*r,usersize)
    val siglist=ratinguser.mapValues{x=>
      //hashtable(1).foreach(println)
      var signatures = ListBuffer.empty[Int]
      for(table <- hashtable){
        var minimum = 10000
        for(user <- x)
          minimum = scala.math.min(table(user), minimum)
        signatures += minimum
      }
      signatures.toList
    }
    //siglist.foreach(println)
    val div=siglist.mapValues(_.sliding(r, r).toList.zipWithIndex).flatMap{case(key,value) =>{
      var l = ListBuffer.empty[(Int, (List[Int], Int))]
      for (e <- value)
        l += ((e._2,(e._1, key)))
      l.toList
    }}.groupByKey().mapValues(x =>
      x.groupBy(_._1).mapValues(_.map(_._2).toList).filter(_._2.size >= 2).flatMap(x => groupinpair(x._2))
    ).flatMap(x => x._2).distinct().map(x => x ::: cosine(user(x(0).toInt)._2, user(x(1).toInt)._2,usersize)).filter(_(2).toFloat >= 0.5).collect().sortBy(x => (x(0).toInt, x(1).toInt))

    div.take(3).foreach(println)




    val groundtruth = sc.textFile("video_small_ground_truth_cosine.csv").map(_.split(",")).map(x => (x(0), x(1))).collect.toSet
    val predict = div.length.toFloat
    val real = groundtruth.size.toFloat
    val g =  groundtruth.intersect(div.map(x=>(x(0),x(1))).toSet).size.toFloat
    println("precision = "+ (g/predict))
    println("recall = "+ (g/real))

    val pw = new PrintWriter(new File(args(1))) //IChen_Yeh_ SimilarProducts_Cosine.txt
    for(o <- div)
      pw.write("\n"+o.mkString(","))
    pw.close()


    /*

        val can = ratinguser.mapValues(users => {
          var siglist = ListBuffer.empty[Int]
          for (user<-users) {

            for (line <- hashtable) {
              var minValue = 10000
              minValue = scala.math.min(line(user), minValue)
              siglist += minValue
            }
          }
          siglist.toList
        })
        val row_div=can.mapValues(x=>x.sliding(r, r).toList.zipWithIndex)
        row_div.foreach(println)



    */


    //ratinguser.map(x => Vectors.sparse(x).asInstanceOf[SparseVector])


  }

  def hash(times: Int, user: Int): Array[Array[Int]] = {
    val table = Array.tabulate(times+1, user) ((x, y) => y)
    /*
    1 2 3 ..... user
    1 2 3 .....user
    total b*r
    */
    for(i <- 1 to times)
      table(i) = table(i).map(x =>( (i*x)+i)% user)
    table
  }


  def cosine(sig1: Set[Int], sig2: Set[Int],usernum:Int): List[String] = {

    //println(un)
    val a=Array.tabulate(usernum){i=>
      if(sig1.contains(i))
        1.toFloat
      else
        0.toFloat
    }
    val b=Array.tabulate(usernum){i=>
      if(sig2.contains(i))
        1.toFloat
      else
        0.toFloat
    }

    val norm1 = blas.snrm2(usernum, a, 1)
    val norm2 = blas.snrm2(usernum, b, 1)
    //println(norm2)

    List((blas.sdot(usernum, a, 1, b, 1) / norm1 / norm2).toString)


  }


  /*
    val file = new File(args(1))
    file.getParentFile().mkdir()
    val txt = new PrintWriter(file)
  */
  def groupinpair(can: List[Int]): List[List[String]] = {
    var newCom = ListBuffer.empty[List[String]]
    can.map(c1 => {
      can.foreach(c2 => {
        if(c1 != c2)
          newCom += List(c1.toString, c2.toString).sorted
      })
    })
    newCom.toList
  }



  /*

    def hash(listofuser: List[Int], m: Int, b: Int, r: Int): List[Int] = {
      //var b=new Random().nextInt(m-1)
      var l = ListBuffer.empty[List[Int]]
      listofuser.foreach{x=>

      }

      /*
      for (i <- 1 until b * r) {
        for (ele <- x) {
          var hashnum = ((i * ele) + i) % m
          l += hashnum
        }
        }
        */

      l.toList
    }
    */
  /*
    def signature(users: List[Int], hash_tables: List[Int]):List[Int] = {
      //hash
      var sigList = ListBuffer.empty[Int]
      for(table <- hash_tables){
        var mininum = 10000
        for(user <- users)
          min_hash = scala.math.min(table(user), min_hash)
        sigList += min_hash
      }
      sigList.toList
    }
  */
}



