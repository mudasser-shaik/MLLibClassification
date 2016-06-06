import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.{SparkContext, SparkConf}

import org.apache.spark.mllib.classification.LogisticRegressionWithSGD
import org.apache.spark.mllib.classification.SVMWithSGD
import org.apache.spark.mllib.classification.NaiveBayes
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.tree.configuration.Algo
import org.apache.spark.mllib.tree.impurity.Entropy
/**
  * Created by mudasser on 01/06/16.
  */
object StumpleUponClassification {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("StumpleUponClassification")
                              .setMaster("local[*]")
                              .set("spark.executor.memory","4g")

    val sc = new SparkContext(conf)

    val textData = sc.textFile("train_NoHeader",3)
    val textRDD =textData.map(line => line.split("\t"))

    val dataLabels = textRDD.map{ row =>

      val trimData = row.map(_.replaceAll("\"", "")) // remove the unwanted Quotes (")
      val label = trimData(row.size-1).toInt         // last column as Label
      val feature = trimData.slice(4,row.size-1)     // replacing "?" with 0.0 and feature from 5 to 25 columns
                            .map(d => if (d == "?") 0.0 else d.toDouble)
                            .map( d => if(d < 0) 0.0 else d)   // removing -ve value for naive Bayes

      LabeledPoint(label, Vectors.dense(feature))
    }.cache()

    val totalDataLabels = dataLabels.count()

    //training the classification models
    val noOfIterations = 10
    val logRegressModel = LogisticRegressionWithSGD.train(dataLabels,noOfIterations)
    val svmModel = SVMWithSGD.train(dataLabels,noOfIterations)

    val naiveBayesModel = NaiveBayes.train(dataLabels)

    val maxTreeDepth =5
    val dTreeModel = DecisionTree.train(dataLabels,Algo.Classification,Entropy,maxTreeDepth)


    // predicting the models
    val lrPredData = logRegressModel.predict(dataLabels.map(lp => lp.features))
    val svmPredData = svmModel.predict(dataLabels.map(lp => lp.features))
    val nbPredData = naiveBayesModel.predict(dataLabels.map(lp=> lp.features))
    val treePredData = dTreeModel.predict(dataLabels.map(lp => lp.features))


    //calculating accuracy of the Models
    val totalCorrectPred = dataLabels.map( lp =>
      if(logRegressModel.predict(lp.features) == lp.label) 1 else 0
    ).sum

    val totalCorrectSVMPred = dataLabels.map( lp =>
      if(svmModel.predict(lp.features) == lp.label) 1 else 0
    ).sum

    val NBCorrectPred = dataLabels.map( lp =>
      if(naiveBayesModel.predict(lp.features) == lp.label) 1 else 0
    ).sum

    val dtreeCorrectPred = dataLabels.map(lp =>
        if(dTreeModel.predict(lp.features) == lp.label) 1 else 0
    ).sum


    val lrAccuracy = totalCorrectPred / totalDataLabels
    val svmAccuracy = totalCorrectSVMPred / totalDataLabels
    val nbAccuracy = NBCorrectPred / totalDataLabels
    val dtreeAccuray = dtreeCorrectPred / totalDataLabels

    println(s"Accuracy of Logistic Regression Model $lrAccuracy")
    println(s"Accuracy of SVM $svmAccuracy")
    println(s"Accuracy of Navie Bayes $nbAccuracy")
    println(s"Accuracy of Decision Tree $dtreeAccuray")

  }


}
