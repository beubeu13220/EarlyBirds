package utils

import java.sql.Timestamp
import java.util.concurrent.TimeUnit

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{asc, row_number, udf}

/**
  * Created by a.brehelin on 19/10/2017.
  */
object DataHelper {

  /**
    * UDF qui permet d'appeler la fonction computeTimeBetweenDate1AndDate2Date dans un withColumn
    * @return date1 - date2
    */
  def dateDiffUDF() = udf[java.lang.Double, java.sql.Timestamp, java.sql.Timestamp, String] {
    (date1, date2, time_unit) =>
      if (date1 == null || date2 == null) null
      else
        computeTimeBetweenDate1AndDate2Date(date1, date2, time_unit)
  }

  /**
    *
    * @param date_date1
    * @param date_date2
    * @param time_unit .on peut choisir différentes périodes pour le format de retour
    *                  - "s" for seconds
    *                  - "m" for minute
    *                  - "h" for hour
    *                  - "d" for day
    *                  - "M" for month
    *                  - "y" for year
    * @return date1 - date2  avec le bon format
    */
  def computeTimeBetweenDate1AndDate2Date(date_date1: Timestamp,
                                          date_date2: Timestamp,
                                          time_unit: String): Double = {

    val delta = date_date1.getTime - date_date2.getTime
    val deltaMillis = TimeUnit.MILLISECONDS.toMillis(delta)
    val deltaSeconds = deltaMillis / 1000.0
    val deltaDays = deltaSeconds / 86400.0
    val result = time_unit match {
      case "s" => deltaSeconds
      case "m" => deltaSeconds / 60.0
      case "h" => deltaSeconds / 3600.0
      case "d" => deltaDays
      case "M" => deltaDays / 30.0
      case "y" => deltaDays / 365.25
      case _ => throw new scala.Exception("Unknown time_unit (" + time_unit + ")")
    }
    result
  }

  /**
    *
    * @param df
    * @param colNameIn : Id string format
    * @param colNameOut : new Id as integer
    * @return dataFrame with old string column and with new integer index column
    */
  def fromIdToIntegerID(df: DataFrame,colNameIn : String , colNameOut : String): DataFrame ={
    // méthode pas très optimale car on charge tout les ID sur un seul noeud
    df
      .select(colNameIn)
      .distinct
      .withColumn(colNameOut, row_number().over(Window.partitionBy().orderBy(asc(colNameIn)))-1)

  }

}
