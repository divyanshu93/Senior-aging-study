import org.apache.spark.ml.regression.IsotonicRegression
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors
import org.apache.commons.csv
import org.apache.spark.{SparkConf, SparkContext}
// $example on$
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.regression.LinearRegressionModel
import org.apache.spark.mllib.regression.LinearRegressionWithSGD
import org.apache.spark.mllib.classification.{LogisticRegressionModel, LogisticRegressionWithLBFGS}
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.util.MLUtils

//running AWS
val AccessKey = "Placeholder"
val SecretKey = "Placeholder"
val EncodedSecretKey = "Placeholder"
val AwsBucketName = "Placeholder"
val MountName = "Placeholder"

//Cluster Deets: Spark 1.6.1 Hadoop 1

/* MISC REFERENCES 
http://www.cakesolutions.net/teamblogs/spark-mllib-linear-regression-example-and-vocabulary
http://spark.apache.org/docs/latest/mllib-isotonic-regression.html
*/

//IF CLUSTER NOT INITIALIZED, USE
//dbutils.fs.mount(s"s3a://$AccessKey:$EncodedSecretKey@$AwsBucketName", s"/mnt/$MountName")

//TO USE DIFFERENT TABLES, ALL YOU HAVE TO DO IS CHANGE s"/mnt/$MountName/correctformat/mini_one_label.csv" TO THE TABLE DIRECTORY SHOWN WHEN YOU UPLOAD ON DATABRICKS
//The mini_one_label.csv is to avoid overloading DataBricks, but is of sufficient complexity for one class to be workable
//The remaining files currently on AWS have been made public, including updates as of Saturday night: 
// /correctformat/mini_one_label.csv <-Is actually working, please investigate why we are getting Strings instead of Doubles or Ints for other files!
// /correctformat/p_stand.csv
// /public_data/*            <- structure preserved
// /train/*                  <- structure preserved
// /result1.csv
// /result2.csv

val csvinit = sc.textFile(s"/mnt/$MountName/result8.csv")
val csvfile = csvinit.map(x=>x.replaceAll("\"", ""))//cleaning
//need number of columns at runtime as below to facilitate testing
val header = csvfile.first() //extract header
val numcols = header.count(_ == ',') + 1 
val ourdata = csvfile.filter(_ !=header)

//used to iterate through targets
val splitheader = header.split(",")
val targetindex = splitheader.indexWhere( _ == "a_ascend")

//for every target in the list
for( x <- List.range(targetindex, numcols)){
  
  //map the data to format accepted <target:LabeledPoint, Vector(attributes)>
  val ourparsedData = ourdata.map { line =>
    val ourparts = line.split(',')
    LabeledPoint(
      //label
      if(ourparts(x).trim.isEmpty) 0.0
        else ourparts(x).toDouble, 

      //features
    Vectors.dense(
      for(col <- Array.range(0, x)) yield {
        if(ourparts(col).trim.isEmpty) 0.0
        else ourparts(col).toDouble
      }
    ))
  }

  //split into test and train data
  val Array(training, test) = ourparsedData.randomSplit(Array(0.8, 0.2))
  training.cache()
  test.cache()

  /******************************************************************************
                                                          LinearRegressionWithSGD_
  ******************************************************************************/
  val LinearRegressionWithSGD_numIterations = 250
  val LinearRegressionWithSGD_stepSize = 0.00000001
  val LinearRegressionWithSGD_model = LinearRegressionWithSGD.train(
    training, 
    LinearRegressionWithSGD_numIterations, 
    LinearRegressionWithSGD_stepSize)

  // Evaluate model on training examples and compute training error
  val LinearRegressionWithSGD_valuesAndPreds = test.map { point =>
  val LinearRegressionWithSGD_prediction = LinearRegressionWithSGD_model.predict(point.features)
    (point.label, LinearRegressionWithSGD_prediction)
  }

  //mean squared error on probabilities
  val LinearRegressionWithSGD_MSE = LinearRegressionWithSGD_valuesAndPreds.map{ 
    case(v, p) => math.pow((v - p), 2) 
    }.mean()
  println("Mean Squared Error for " + splitheader(x) + " = " + LinearRegressionWithSGD_MSE)
}