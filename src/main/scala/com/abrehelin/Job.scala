package com.abrehelin

/**
  * Created by a.brehelin on 18/10/2017.
  */


import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import utils.DataHelper._
import utils.ExportHelper._
import com.abrehelin.Constants._


object Job {


  def main(args: Array[String]): Unit ={

    val spark = SparkSession
      .builder
      .master("local")
      .appName("spark test")
      .getOrCreate()

    // Définition du schema de la table en entrée, donc avec long pour le timestamp
    val customSchema = StructType(Array(
      StructField("userID", StringType, true),
      StructField("itemID", StringType, true),
      StructField("rating", DoubleType, true),
      StructField("timestamp", LongType, true)))

    // Import de la table avec le path mentionné en argument
    val df = spark.read
      .format("com.databricks.spark.csv")
      .option("header", "false")
      .schema(customSchema)
      .csv(args(0))

    // On cache les deux tables loopUp pour les calculer 1 fois au lieu de 2
    val lookUpUser = fromIdToIntegerID(df,"userID","userIDAsInteger").cache()
    val lookUpProduct = fromIdToIntegerID(df,"itemID","itemIDAsInteger").cache()

    val dfTransform = df
      //on cast le timestamp long en Timestamp afin d'utiliser notre fonction dateDiffUdf
      .withColumn("timestamp", (col("timestamp")/timestampScale).cast(TimestampType))
      //on get le max timestamp de tout le DF (je ne sais pas si il fallait le max de tout le DF ou le max par userID)
      //la modification n'est pas compliquée avec les windows et moins couteuse si on crée des partitions par userID
      .withColumn("maxTimestamp", max(col("timestamp")).over)
      .withColumn("diffDays", dateDiffUDF()(col("maxTimestamp"),col("timestamp"),lit("d")))
      // On s'assure d'avoir des entiers pour notre calcul de puissance qui suit
      .withColumn("diffDaysNorm", round(col("diffDays")))
      .withColumn("penRating", col("rating")*pow(penValue, "diffDaysNorm"))
      .filter(col("penRating")>thresholdRating)

    val aggRatings = dfTransform
      // 2 Jointures pour récupérer les users & items en integer, puis un groupBy
      // pour calculer la somme des ratings par users et items
      // on cache pour ne pas calculer 2 fois, une fois à l'export et une fois au test
      .join(lookUpUser, List("userID"), "left")
      .join(lookUpProduct, List("itemID"), "left")
      .groupBy("userIDAsInteger", "itemIDAsInteger").agg(sum("penRating").alias("ratingSum"))
      .cache()


    // Export DF
    val dfToExport = List(aggRatings, lookUpUser, lookUpProduct)
    val nameToExport = List(nameAggRatings, nameLookUpUser, nameLookUpProduct)

    val map_save = nameToExport.zip(dfToExport).toMap

    map_save.keys.foreach( name => fromDfToExport(map_save(name),args(1),name))

    // Création des logs de test et export dans un fichier txt
    // Peut être optimisé
    testResults += testUniqueID(df,lookUpUser, List(col("userID")))
    testResults += testUniqueID(df,lookUpProduct, List(col("itemID")))
    testResults += testUniqueID(df,aggRatings, List(col("userID"),col("itemID")))

    fromTestResultToSave(testResults, args(1),"outTestExport")

    // on a caché tout les DF donc on les unpersist
    dfToExport.foreach(df=>df.unpersist())

  }

  // Fonction de test pour vérifier que le nombre de ligne correspond bien
  def testUniqueID(df : DataFrame, uniqueDf : DataFrame, id:List[Column]): String ={
    val nbDistinctID = df.select(id:_*).distinct.count()
    val nbRowUniqueDF = uniqueDf.count()
    if (nbRowUniqueDF != nbDistinctID) {
      "KO : Les ID de la table unique des "+ id.toString() + " ne coresspondent pas au nombre d'ID dans le DF initial"
    } else {
      "OK : Les ID de la table unique des "+ id.toString() + " coresspondent au nombre d'ID dans le DF initial"
    }
  }


}
