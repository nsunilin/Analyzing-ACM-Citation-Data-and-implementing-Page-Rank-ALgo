//_____________________________________________________________________________________________
// Preprocessing the data for Page Rank
// 1. Retrieving (index ->references) pairs and saving it in file Link.txt
// 2. Retrieving (title ,index) pairs and saving it in pgTP.txt
//_____________________________________________________________________________________________

import org.apache.hadoop.conf._
import org.apache.hadoop.io._
import org.apache.hadoop.mapreduce.lib.input._
import org.apache.spark.graphx.Graph
import scala.util.MurmurHash
@transient val conf=new Configuration
conf.set("textinputformat.record.delimiter","#*")
val inputrdd=sc.newAPIHadoopFile("/hadoop-user/data/citation-acm-v8.txt",classOf[TextInputFormat],classOf[LongWritable],classOf[Text],conf).map{case(key,value)=>value.toString}.filter(value=>value.length!=0)

val indexLink = inputrdd.map(l=>l.replace("\n",""))
val withref=indexLink.filter(m=>(m.contains("#%")))
val checkwe=withref.map(m=>m.split("#index"))
val complete=checkwe.map(m=>m(1))
val split = complete.map(m=>m.split("#!")(0))  //remove the abstract part
val ids = split.map(m=>m.split("#%")).map(m=>(m(0),m))
val list = ids.flatMapValues(x=>x).filter{case(index,ref)=>(index!=ref)}

list.saveAsTextFile("/hadoop-user/data/Link.txt")


//________________________________________________________________________________________________________________________________________
//2.

val tt =checkwe.map(x=>(x(0).split("#")))  // look up for part before # (before #@ ot #t )
val ttname = tt.map(x=>x(0))
val tt1 = checkwe.map(x=>x(1).split("#%")) 
val ttid = tt1.map(x=>x(0))   // consider part before #% i.e index nos.
val title_id = ttname.zip(ttid)  // combine both rdd
title_id.saveAsTextFile("/hadoop-user/data/pgTP")

val hT = title_id.toDF
val names=Map("_1"->"Title","_2"->"paper_id")
val pgTP=hT.select(hT.columns.map(c=>col(c).as(names.getOrElse(c,c))):_*)
pgTP.createOrReplaceTempView("pgTP")

