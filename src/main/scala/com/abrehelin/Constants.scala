package com.abrehelin

import scala.collection.mutable.ListBuffer

/**
  * Created by a.brehelin on 19/10/2017.
  */
object Constants {

  val nameAggRatings = "aggratings"
  val nameLookUpUser = "lookupuser"
  val nameLookUpProduct = "lookup_product"

  val thresholdRating = 0.01
  val penValue = 0.95

  val timestampScale = 1000

  val testResults = ListBuffer[String]()
}
