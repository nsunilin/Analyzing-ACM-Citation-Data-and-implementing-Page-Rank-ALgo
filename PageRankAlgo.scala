//______________________________________________________________________________________________________________
//PART 2 a:   Calculate Indegree Nodes, 
//2.b : Calculate Top - 10 papers with highest page rank
//______________________________________________________________________________________________________________


import org.apache.hadoop.conf._
import org.apache.hadoop.io._
import org.apache.hadoop.mapreduce.lib.input._
import org.apache.spark.graphx.Graph
import scala.util.MurmurHash

sc.setCheckpointDir("hdfs://CSC570BD-BE-Master:9000/hadoop-user/data/data-checkpoint")

//read Link.txt file
val input=sc.textFile("/hadoop-user/data/Link.txt",2).map(x=>x.replace("(",""))
val r1= input.map(x=>x.replace(")",""))
val list=r1.map(m=>m.split(",")).map(m=>(m(0),m(1)))

val pgread = sc.textFile("/hadoop-user/data/pgTP/*",2).map(x=>x.replace("[",""))
val r2= pgread.map(x=>x.replace("]",""))
val pglist=r2.map(m=>m.split(",")).map(m=>(m(0),m(1)))
val pgTb2p = pglist.toDF
val names=Map("_1"->"Title","_2"->"paper_id")
val pgTPP=pgTb2p.select(pgTb2p.columns.map(c=>col(c).as(names.getOrElse(c,c))):_*)
pgTPP.createOrReplaceTempView("pgTP")

//__________________________________________________________________________________________________________________________________________________
//indegreee

val graphList=list.map{case (id,ref) =>(MurmurHash.stringHash(id.toString).toLong,MurmurHash.stringHash(ref.toString).toLong)}
val graph=Graph.fromEdgeTuples(graphList,null)

val indegNodes=graph.inDegrees
val nodes = graph.numVertices
val interchange = indegNodes.map(m=>(m._2,m._1))
val countIndgree = interchange.countByKey()
val result = countIndgree.map(m=>(m._1,(m._2.toDouble/nodes.toDouble)))
sc.parallelize(result.toSeq).saveAsTextFile("/hadoop-user/data/PageIndegree")

//____________________________________________________________________________________________________________________________________________
//2.B

//CALCULATE INLINKS AND OUTLINKS ,Win ,Wout

case class pLinks(ind : String, ref:String)
val newTable8=list.map{case (ind,ref)=>pLinks(ind,ref)}.toDF
newTable8.createOrReplaceTempView("pgtable")

val inLink=spark.sql("select ref,COUNT(ref) as inlink_count from pgtable GROUP BY ref")
val inLinkView=inLink.createOrReplaceTempView("inlink")

val outLink=spark.sql("Select ind,COUNT(ind) as outlink_count from pgtable GROUP BY ind")
val outLinkView=outLink.createOrReplaceTempView("outlink")

val c = spark.sql("select pg.ind,pg.ref,in.inlink_count,ot.outlink_count from pgtable as pg,inlink as in,outlink as ot where pg.ref=in.ref AND ot.ind=pg.ind")

val x1=c.createOrReplaceTempView("comb")
// join inlinks of index , inlinks of ref , outlinks of ref with index and ref
val joinTab=spark.sql("select b.ind,b.ref,b.inlink_count,b.outlink_count,b1.outlink_count as b_outlink from comb as b JOIN  comb as b1 ON  b.ref=b1.ind GROUP BY b.ind,b.ref,b.inlink_count,b.outlink_count,b1.outlink_count")

joinTab.createOrReplaceTempView("joinTab")
//calc denominator of Wout
val w3=spark.sql("select ind,SUM(b_outlink) as denout,SUM(inlink_count) as denin from joinTab GROUP BY ind")
w3.createOrReplaceTempView("w3")
//calulate denominator for Win
val qwe3=spark.sql("select j.ind,j.ref,j.inlink_count,j.outlink_count,j.b_outlink,w.denout,w.denin from joinTab as j,w3 as w where j.ind=w.ind")

qwe3.createOrReplaceTempView("qwe3")

// complete table : ind, ref, inlink_count,outlink_count, Win , Wout with other intermediate step calculations
val qwe4=spark.sql("select ind,ref,inlink_count,outlink_count,b_outlink,denout,denin,(inlink_count/denin) as Win,(b_outlink/denout) as Wout from qwe3")
 
qwe4.createOrReplaceTempView("completeTable")
//---------------------------------------------------------------------------------------------------------------------------------------------

//initalize step of page rank algo
val init=spark.sql("select ind,ref,win,wout,(1/(select count(distinct(ind)) from completeTable)) as pr_ind from completeTable")
init.createOrReplaceTempView("initial")

//calcuate iteration 1
val ite1  =  spark.sql("select ref,(((1-0.85)/(select count(distinct(ind)) from completeTable))+(.85 *sum(pr_ind*Win*Wout))) as PR_ref from initial group by ref")
ite1.createOrReplaceTempView("ite1")

//get page rank for paper1  after iteration 1
val ite1join=spark.sql("select i.ind,i.ref,i.win,i.wout,ite1.PR_ref as pr from initial as i,ite1 where i.ind=ite1.ref")
ite1join.createOrReplaceTempView("ite1comp")

//run iterations 2 to 10 and compute page rank
for(i <- 1 to 9) 
{
val ite2=spark.sql("select ref,(((1-0.85)/(select count(distinct(ind)) from completeTable))+(.85 *sum(pr*win*wout))) as PR_ref from ite1comp group by ref")
ite2.createOrReplaceTempView("ite2")

val ite2join=spark.sql("select i.ind,i.ref,i.win,i.wout,ite2.PR_ref as pr from initial as i,ite2 where i.ind=ite2.ref")
ite2join.createOrReplaceTempView("ite1comp")

}


// order by descending : get top 10
val t10out=spark.sql("select distinct(ind),pr from ite1comp order by pr DESC limit 10")
t10out.createOrReplaceTempView("top10")

val top10_in = spark.sql("select t.ind, i.inlink_count,t.pr from top10 as t ,inlink as i where t.ind = i.ref")
//top10_in.rdd.saveAsTextFile("/hadoop-user/data/top10_in.txt")
top10_in.createOrReplaceTempView("top10_in")

val outpgpr=spark.sql("select p.Title,o.inlink_count,o.pr from top10_in  as o,pgTP as p where o.ind=p.paper_id order by o.pr DESC")
outpgpr.rdd.coalesce(1).saveAsTextFile("/hadoop-user/data/outputFinal")

