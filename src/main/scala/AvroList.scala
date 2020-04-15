package com.pk
/*
Date Created: 02/20/2020
Process to get all details for each order, create Hfiles and bulklaod them into Hbase table
Date of enhancement: 03/23/2020
Creating the row key was done at the view level and for the main table rowkeys are given in the config file.
Date of Enhancement: 03/26/2020
All rows in the table pertaining to a rowkey should be inserted into hbase cell as an avro list

*/

import java.io.{ByteArrayOutputStream, File, IOException}

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.avro.Schema
import org.apache.avro.Schema.Parser
import org.apache.avro.generic.{GenericData, GenericDatumWriter, GenericRecord}
import org.apache.avro.io.EncoderFactory
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.client.{ConnectionFactory, HTable}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.io.compress.Compression
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.mapreduce.{HFileOutputFormat2, LoadIncrementalHFiles}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, KeyValue, TableName}
import org.apache.spark.Partitioner
import org.apache.spark.sql.functions.{lit, _}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}

import scala.collection.JavaConverters._
object AvroList {


  def main(args: Array[String]) {
    val spark = SparkSession.builder()
      .appName("BulkLoadPOC")
      .getOrCreate()
    val inpconf = ConfigFactory.parseFile(new File(args(0)))
    val inpDb = inpconf.getString("inputDatabase")
    val inpTblNm = inpconf.getString("inputTable")
    val inpTblOrPath = inpconf.getString("inpTblOrPath")
    val inpTblPath = inpconf.getString("inpTblPath")
    val inpTblFormat = inpconf.getString("inpTblFormat")
    val primaryKey = inpconf.getString("primaryKey")
    val rkeys = inpconf.getStringList("rowKeyColumns").asScala.toList
    val schemaId = inpconf.getString("schemaId")
    val schemaPath = inpconf.getString("schemaPath")
    val tblNm = inpconf.getString("hbaseTableName")
    val path = inpconf.getString("hfileOutputPath")
    val myAvroSchemaText = new Schema.Parser ().parse (new File (schemaPath)).toString
    val avroSchema = new Schema.Parser().parse (myAvroSchemaText)
    // Get the name of the columns in the avsc file.
    val myColList=avroSchema.getFields.asScala.toList.map(_.name)

    //create Hbase conf
    val conf = HBaseConfiguration.create()
    conf.set(TableOutputFormat.OUTPUT_TABLE, tblNm)
    conf.set("hfile.compression", Compression.Algorithm.SNAPPY.getName)
    savehfile(inpconf)
    saveAsHBaseTbl()

    /*  Function to serialize each row into Avro  */
    def toAvro(rk: String,cq:String,row: Row, inpconf: Config): (ImmutableBytesWritable,KeyValue) = {
      val allfield = row.schema.fields
      val field = allfield(2).name
      val schema = (new Parser).parse(getClass
        .getResourceAsStream(s"/avsc/"+ inpTblNm + "_"+ schemaId + ".avsc"))
      val rowArray = row.getAs[scala.collection.mutable.WrappedArray[Row]](field)

      def toRec(row: Row,schema: Schema):GenericRecord={
        val allfields = row.schema.fields
        val fields = allfields
        val genericAvroRecord = new GenericData.Record(schema)

        for (i <- 0 until row.size) {
          val field = fields(i)
          if (row.isNullAt(i))
            genericAvroRecord.put(field.name, null)
          else
            field.dataType match {
              // Binary & Byte Types yet to be tested
              case BinaryType => genericAvroRecord.put(field.name, row.getAs[Array[Byte]](i))
              case ByteType => genericAvroRecord.put(field.name, row.getByte(i))
              case ShortType => genericAvroRecord.put(field.name, row.getShort(i).toInt)
              case IntegerType => genericAvroRecord.put(field.name, row.getInt(i))
              case LongType => genericAvroRecord.put(field.name, row.getLong(i))
              case FloatType => genericAvroRecord.put(field.name, row.getFloat(i))
              case DoubleType => genericAvroRecord.put(field.name, row.getDouble(i))
              case StringType => genericAvroRecord.put(field.name, row.getString(i))
              case BooleanType => genericAvroRecord.put(field.name, row.getBoolean(i))
              case _: DecimalType => genericAvroRecord.put(field.name, row.getDecimal(i).toString)
              case TimestampType => genericAvroRecord.put(field.name, row.getTimestamp(i).getTime)
              case DateType => genericAvroRecord.put(field.name, row.getDate(i))
            }
        }
        genericAvroRecord
      }
      val avroRecord=rowArray.toArray.map(x=> toRec(x,schema))


      val bytesAr = {
        val out = new ByteArrayOutputStream()
        val encoder = EncoderFactory.get.binaryEncoder(out, null)
        val writer = new GenericDatumWriter[GenericRecord](avroRecord.head.getSchema)
        try {
          avroRecord.foreach(x=> writer.write(x, encoder))
          encoder.flush()
          out.close()
        } catch {
          case e: IOException =>
            e.printStackTrace()
        }
        out.toByteArray
      }

      val key = Bytes.toBytes(rk + '$' + cq)
      val kv = new KeyValue(Bytes.toBytes(rk),
        Bytes.toBytes("cf"),
        Bytes.toBytes(cq),
        bytesAr)
      (new ImmutableBytesWritable(key), kv)
    }
   // where("myRowKey='20200101~00001~000008550'")
    /*     Function to save as Hfiles     */
    def savehfile(inpconf: Config) {
      val df = if (inpTblOrPath =="File") {
        val fdf= spark.read.format(inpTblFormat).load(inpTblPath)
        return fdf}
      else {spark.sql("select concat_ws('~'," + rkeys.mkString(",") + ") as myRowKey,"+myColList.mkString(",")+" from " + inpDb + "." + inpTblNm )}

      df.show(false)
      val tblDf = df.withColumn("colname",lit(inpTblNm))
        .groupBy("myRowkey","colname")
        .agg(collect_list(struct(myColList.map(c => col(c)): _*)).alias("Array_List"))
      tblDf.show(false)
      tblDf.printSchema()
      //val finalDf = tblDf.drop(rKeys.mkString(","))
      val finalDf = tblDf

      val kvRdd = finalDf.rdd.map(x => toAvro(x(0).toString,x(1).toString,x, inpconf))
      kvRdd.take(20).foreach(println)
      //System.exit(0)

      /* Create 4 splits and get 4 start keys*/
      val n = inpconf.getInt("regionSplits")

      /* Add $ and the column name to the key, so that it can be sorted and then it will be easy to remove before saving as Hfile */
      val finalrkRdd = finalDf.rdd.map(x => x(0) + "$" + x(1))
      val expectedSize = finalrkRdd.count / n
      val indices = Stream.iterate(0D)(x => x + expectedSize).map(math.round).take(n).toList
      val splitKeys = finalrkRdd.sortBy(x => x).zipWithIndex.filter(x => indices.contains(x._2)).map(_._1).collect.map(_.getBytes)
      /* created a custom partitioner class to split into 4 */
      class datasetPartitioner(splits: Array[Array[Byte]]) extends Partitioner {
        override def getPartition(key: Any): Int = {
          val k = key
          for (i <- 1 until splits.length)
            if (Bytes.compareTo(Bytes.toBytes(k.toString()), splits(i)) < 0) return i - 1
          splits.length - 1
        }
        override def numPartitions: Int = splits.length
      }
      /* Repartition and sort */
      val reptRdd = kvRdd.repartitionAndSortWithinPartitions(new datasetPartitioner(splitKeys))
      /* Remove column name from row key. */
      val repartRdd = reptRdd.map(x => (new ImmutableBytesWritable(Bytes.toStringBinary(x._1.get()).split("\\$")(0).getBytes), x._2))
      /* Create Hbase configuration and save the Hfile  */

      repartRdd.saveAsNewAPIHadoopFile(path,
        classOf[ImmutableBytesWritable],
        classOf[KeyValue],
        classOf[HFileOutputFormat2],
        conf)

    }

    /*    Function to move hfles to Hbase     */
    def saveAsHBaseTbl() = {
      /* Load it to Hbase table */
      val loader = new LoadIncrementalHFiles(conf)
      val connection = ConnectionFactory.createConnection(conf)
      val table = connection.getTable(TableName.valueOf(tblNm)).asInstanceOf[HTable]
      try {
        loader.doBulkLoad(new Path(path), table)
      }
      catch {
        case except => println("Not able to complete bulk load exception occured", except.printStackTrace)
      }
      finally {
        connection.close()
        table.close()
        spark.close()
      }
    }

  }
}


