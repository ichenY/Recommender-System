import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.Rating
import java.io._
import java.io.{File, PrintWriter}



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

