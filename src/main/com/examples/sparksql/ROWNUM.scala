/////////////////////////////////////////////////////////////////////////////
//  Scala File Name   : CreateDataFrame
//  Author            : Laxminarayana Cheruku
//  Creation Date     : 26/APR/2016
//  Usage             : Implement ROWNUM 
/////////////////////////////////////////////////////////////////////////////
package com.examples.sparksql

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object ROWNUM {
  
  def main(args: Array[String]){
    
    //Create Spark Conf
    val sparkConf = new SparkConf().setAppName("ROWNUM-Implementation").setMaster("local")
    
    //Create Spark Context - sc
    val sc = new SparkContext(sparkConf)
    
    //Create Sql Context
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)    
    
    //Import Sql Implicit conversions
    import sqlContext.implicits._
    import org.apache.spark.sql.Row
    import org.apache.spark.sql.types.{StructType,StructField,StringType}   
    
    //Read Data and Create Row RDD
    val data_rdd = sc.textFile("G:\\Spark-Blog-Posts\\sample.csv")
    val data_row_rdd = data_rdd.map(x => x.toString().split(",")).map(p => Row.fromSeq(p.toSeq))
    
    //Create Schema RDD
    val schema_string = "name,id"
    val schema_rdd = StructType(schema_string.split(",").map(fieldName => StructField(fieldName, StringType, true)) )
    
    //Create DataFrame
    val sample_df = sqlContext.createDataFrame(data_row_rdd, schema_rdd)
    
    //Register "People" Table on DataFrame
    sample_df.registerTempTable("People")
    
    //Now, implement "rownum" using limit
    val res = sqlContext.sql("select * from People limit 2")
    res.show
    
  }
  
}