package utils

import org.apache.spark.sql.{DataFrame, SaveMode}
import java.io._
import scala.collection.mutable.ListBuffer

/**
  * Created by a.brehelin on 19/10/2017.
  */
object ExportHelper {

  /**
    *
    * @param df
    * @param path : directory to save the csv
    * @param name : name csv to export
    * @return DataFrame
    */
  def fromDfToExport(df:DataFrame, path:String, name:String): Unit ={
    df
      .write
      .mode(SaveMode.Overwrite)
      .option("header", "true")
      .format("com.databricks.spark.csv")
      .save(path+name+".csv")
  }

  def fromTestResultToSave(testResult:ListBuffer[String], path:String,name :String ): Unit={
    val stringTosave = testResult.mkString("\n")
    val writer = new PrintWriter(new File(path+name+".log"))
    writer.write(stringTosave)
    writer.close()
  }

}
