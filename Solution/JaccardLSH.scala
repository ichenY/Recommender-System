import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.math._
import scala.util.Sorting
import scala.io.Source
import scala.util.Random
import scala.collection.mutable.ListBuffer
import java.io._

import org.apache.spark.mllib.recommendation.{ALS, Rating}
import com.github.fommil.netlib.BLAS.{getInstance => blas}



object JaccardLSH {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("hw3").setMaster("local[3]")
    val sc = new SparkContext(sparkConf)
    val path0=args(0)
    val csv = sc.textFile(args(0))
    val header = csv.first()
    val b = 10
    val r = 5
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
        x.groupBy(_._1).mapValues(_.map(_._2).toList).filter(_._2.size >= 2) // x._2.toString.)//.flatten.sliding(2,2)//.foreach{x =>x.toList}//.toList
       .flatMap(x => groupinpair(x._2))
      )
      .flatMap(x => x._2).distinct().map(x => x ::: jaccard(user(x(0).toInt)._2, user(x(1).toInt)._2)).filter(_(2).toDouble >= 0.5).collect().sortBy(x => (x(0).toInt, x(1).toInt))

    div.take(3).foreach(println)




    val groundtruth = sc.textFile("video_small_ground_truth_jaccard.csv").map(_.split(",")).map(x => (x(0), x(1))).collect.toSet
    val predict = div.length.toDouble
    val real = groundtruth.size.toDouble
    val g =  groundtruth.intersect(div.map(x=>(x(0),x(1))).toSet).size.toDouble
    println("precision = "+ (g/predict))
    println("recall = "+ (g/real))

    val pw = new PrintWriter(new File(args(1))) //IChen_Yeh_ SimilarProducts_Jaccard.txt
    for(o <- div)
      pw.write("\n"+o.mkString(","))
    pw.close()
    /*
    val a=div.map(_.toString())
    a.foreach(pw.println)
    pw.close()

*/
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


  def jaccard(sig1: Set[Int], sig2: Set[Int]): List[String] = {
    val similar = sig1.intersect(sig2).size.toDouble
    val total = sig1.union(sig2).size.toDouble
    List((similar/total).toString)
  }


/*
  val file = new File(args(1))
  file.getParentFile().mkdir()
  val txt = new PrintWriter(file)
*/
  def groupinpair(can: List[Int]): List[List[String]] = {
    var newComb = ListBuffer.empty[List[String]]
    can.map(c1 => {
      can.foreach(c2 => {
        if(c1 != c2)
          newComb += List(c1.toString, c2.toString).sorted
      })
    })
    newComb.toList
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

object ModelBasedCF {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("task2").setMaster("local[1]")
    val sc = new SparkContext(sparkConf)
    val testfile = sc.textFile(args(1))
    val trainfile = sc.textFile(args(0))
    val head1 = testfile.first()
    val head2 = trainfile.first()



    val testdata=testfile.filter(x=>x!=head1).map(x=>x.split(",") match {case Array(user, product,rating)=>Rating(user.toInt,product.toInt,0)})
    val traindata=trainfile.filter(x=>x!=head2).map(x=>x.split(",") match {case Array(user, product,rating,time)=>Rating(user.toInt,product.toInt,rating.toDouble)})
    val sub_train=traindata.subtract(testdata)
    //traindata.foreach(println)
    //val join_test=testdata.map(x=>((x.user,x.product),x.rating))
    //val join_train=traindata.map(x=>((x.user,x.product),x.rating))

    //Model
    val rank = 10
    val numIterations = 20
    val model = ALS.train(sub_train, rank, numIterations, 0.5) //10 20 0.3 5 10 0.3
    val p_test=testdata.map(x=>(x.user,x.product))
    val out_prediction=model.predict(p_test)//.map{case Rating(x,y,z) => ((x, y), z)}//if (z<0) ((x,y), 0.0) else if (z>5) ((x, y), 5.0) else
    val prediction=out_prediction.map{case Rating(x,y,z) => ((x, y), z)}
    val sort_out=out_prediction.sortBy(x=>(x.user,x.product))

    val p_traindata=sub_train.map(x=>((x.user,x.product),x.rating))

    val test_rate=testfile.filter(x=>x!=head1).map(x=>x.split(",") match {case Array(user, product,rating)=>Rating(user.toInt,product.toInt,rating.toDouble)})
    val j_test=test_rate.map(x=>((x.user,x.product),x.rating))

    val table=j_test.join(prediction)
    val minimum=table.map(x=>x._2._2).collect.min
    val maximum=table.map(x=>x._2._2).collect.max

    val MSE = table.map{case((user, product), (r1, r2)) =>
      val err = r1-((r2-minimum)/(maximum-minimum)*4+1)
      err*err
    }.mean()

    val  RMSE=math.sqrt(MSE)
    //print(RMSE)


    val diff= table.map { case ((user, product), (r1, r2)) => math.abs(r1-((r2-minimum)/(maximum-minimum)*4+1))}.collect

    var num1=0
    var num2=0
    var num3=0
    var num4=0
    var num5=0

    for (number <- diff) {
      number match {
        case number if (number>=0 && number<1) => num1 = num1 + 1;
        case number if (number>=1 && number<2) => num2 = num2 + 1;
        case number if (number>=2 && number<3) => num3 = num3 + 1;
        case number if (number>=3 && number<4) => num4 = num4 + 1;
        case number if (number>=4 ) => num5 = num5 + 1;
      }
    }

    println(">=0 and <1:"+ num1)
    println(">=1 and <2:"+ num2)
    println(">=2 and <3:"+ num3)
    println(">=3 and <4:"+ num4)
    println(">=4 :"+ num5)
    println("RMSE = " + RMSE)



    val g=sort_out.map(x=>(x.user,x.product,x.rating))//.foreach(println)
    // g.collect()
    //"IChen_Yeh_ModelBasedCF.txt")

    val pw = new PrintWriter(new File(args(2)))
    pw.println("user product prediction")
    val a = g.map(_.toString()).collect()
    a.foreach(pw.println)


    pw.close()







  }

}
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






