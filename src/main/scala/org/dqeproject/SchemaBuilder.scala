package org.dqeproject

import org.apache.spark.sql.types.{StringType, StructField, StructType, TimestampType,LongType,IntegerType,DoubleType,ShortType,DecimalType,BooleanType,DateType,FloatType}

class SchemaBuilder() {

  def schemaBuild(schema:Seq[scala.collection.immutable.Map[String,String]]) ={

    val builtSchema = schema.map{
      case x=>
        val colName = x.get("colname")
        val colType = x.get("coltype")
        val colnull = x.get("colnull")
        if (colType.toString.toLowerCase() == "timestamptype"){
          StructField(colName.get,TimestampType,true)
        }
        else if (colType.toString.toLowerCase() == "stringtype"){
          StructField(colName.get,StringType,true)
        }
        else if (colType.toString.toLowerCase() == "longtype") {
          StructField(colName.get, LongType, true)
        }
        else if (colType.toString.toLowerCase() == "integertype") {
          StructField(colName.get, IntegerType, true)
        }
        else if (colType.toString.toLowerCase() == "floattype") {
          StructField(colName.get, FloatType, true)
        }
        else if (colType.toString.toLowerCase() == "booleantype") {
          StructField(colName.get, BooleanType, true)
        }
        else if (colType.toString.toLowerCase() == "doubletype") {
          StructField(colName.get, DoubleType, true)
        }
        else if (colType.toString.toLowerCase() == "shorttype") {
          StructField(colName.get, ShortType, true)
        }
        else if (colType.toString.toLowerCase() == "datetype") {
          StructField(colName.get, DateType, true)
        }
        else{
          StructField(colName.get,StringType,true)
        }
    }
    StructType(builtSchema)
  }

}
