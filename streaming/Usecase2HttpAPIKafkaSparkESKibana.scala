package streaming

import org.apache.spark.sql._
import org.apache.spark.SparkContext
import org.apache.spark.streaming.{Seconds, StreamingContext}
import StreamingContext._
import org.apache.spark.sql.types._
// ES packages
import org.elasticsearch.spark.sql._

// Kafka packages
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
// HTTPS Rest API -> Nifi -> Kafka -> Spark Streaming (complex JSON) -> DF-> Elastic Search -> Kibana Dashboard

object Usecase2HttpAPIKafkaSparkESKibana extends App {
		val spark=SparkSession.builder().enableHiveSupport().appName("Usecase1 kafka to sql/nosql")
												.master("local[*]")
												.config("spark.history.fs.logDirectory", "file:///tmp/spark-events")
												.config("spark.eventLog.dir", "file:////tmp/spark-events")
												.config("spark.eventLog.enabled", "true") // 3 configs are for storing the events or the logs in some location, so the history can be visible
								//				.config("hive.metastore.uris","thrift://localhost:9083") //hive --service metastore
								//				.config("spark.sql.warehouse.dir","hdfs://localhost:54310/user/hive/warehouse") //hive table location
												.config("spark.sql.shuffle.partitions",10)
												.config("spark.es.nodes","localhost")
												.config("spark.es.port","9200")
												.config("es.nodes.wan.only",true)
												.enableHiveSupport().getOrCreate();

		val ssc=new StreamingContext(spark.sparkContext,Seconds(1))

				spark.sparkContext.setLogLevel("error")


		val weblogschema = StructType(Array(
						StructField("username", StringType, true),
						StructField("ip", StringType, false),
						StructField("dt", StringType, true),
						StructField("day", StringType, true),    
						StructField("month", StringType, true),
						StructField("time1", StringType, true),
						StructField("yr", StringType, true),
						StructField("hr", StringType, true),
						StructField("mt", StringType, true),
						StructField("sec", StringType, true),
						StructField("tz", StringType, true),
						StructField("verb", StringType, true),
						StructField("page", StringType, true),
						StructField("index", StringType, true),
						StructField("fullpage", StringType, true),
						StructField("referrer", StringType, true),
						StructField("referrer2", StringType, true),
						StructField("statuscd", StringType, true)));  


		val topics = Array("hvacsensor1");

		val kafkaParams = Map[String, Object](
				"bootstrap.servers" -> "localhost:9092,localhost:9093,localhost:9094",
				"key.deserializer" -> classOf[StringDeserializer],
				"value.deserializer" -> classOf[StringDeserializer],
				"group.id" -> "we37",
				"auto.offset.reset" -> "latest"
				,"enable.auto.commit" -> (false:java.lang.Boolean) )

				val dstream = KafkaUtils.createDirectStream[String, String](ssc,
						PreferConsistent, 
						Subscribe[String, String](topics, kafkaParams))

try {
				import spark.sqlContext.implicits._
				dstream.foreachRDD{
			rddfromdstream =>
			val offsetranges=rddfromdstream.asInstanceOf[HasOffsetRanges].offsetRanges
			println("Printing Offsets")
			offsetranges.foreach(println)
			
			if(!rddfromdstream.isEmpty())
			{
			  
      			val jsonrdd=rddfromdstream.map(x=>x.value())  
						val jsondf =spark.read.option("multiline", "true").option("mode", "DROPMALFORMED").json(jsonrdd)
						
																  
								  //val userwithid= jsondf.withColumn("results",explode($"results")).select("results[0].username")
								  jsondf.printSchema();
								  jsondf.createOrReplaceTempView("apiview")
								  //jsondf.show(5,false)
//spark.sql("select * from apiview").show(10,false)
//{"results":[{"gender":"female","name":{"title":"Miss","first":"Olivia","last":"Hall"},
//"location":{"street":{"number":660,"name":"Anzac Drive"},"city":"Whangarei","state":"Marlborough","country":"New Zealand","postcode":97893,"coordinates":{"latitude":"63.4187","longitude":"23.3510"},"timezone":{"offset":"0:00","description":"Western Europe Time, London, Lisbon, Casablanca"}},"email":"olivia.hall@example.com","login":{"uuid":"c30710f6-fd79-4f52-a3be-86eac58a7df1","username":"whiteleopard456","password":"puck","salt":"1BI7ujkn","md5":"5eeb388fe9b40550d941a9f4071c2c67","sha1":"9fbd40dbf00523d22bc84b0bcd91116c798a8163","sha256":"e6069402465d482286e6037886ad5a1bdc3632f8d2ac5fa1164968862b65be8e"},"dob":{"date":"1992-11-25T03:52:39.891Z","age":29},"registered":{"date":"2013-10-05T13:27:07.059Z","age":8},"phone":"(356)-489-1410","cell":"(903)-340-8070","id":{"name":"","value":null},"picture":{"large":"https://randomuser.me/api/portraits/women/73.jpg","medium":"https://randomuser.me/api/portraits/med/women/73.jpg","thumbnail":"https://randomuser.me/api/portraits/thumb/women/73.jpg"},"nat":"NZ"}],"info":{"seed":"674a12c5263f06bd","results":1,"page":1,"version":"1.3"}}
			
println("Raw SQL")
spark.sql("""select results,
  explode(results) as res,
  info.page as page,res.cell as cell,
  res.name.first as first,
  res.dob.age as age,
  res.email as email,res.location.city as uscity,res.location.coordinates.latitude as latitude,
  res.location.coordinates.longitude as longitude,res.location.country as country,
  res.location.state as state,
  res.location.timezone as timezone,res.login.username as username
 from apiview  """).show(10,false)								  
import org.apache.spark.sql.functions._
println("Data to write into Elastic search for analysis") 
val esdf=spark.sql("""
  select username as custid,page,cell,first,age,email,uscity,concat(latitude,',',longitude) as coordinates,
  regexp_replace(cell,'[  ()-]','') as cellnumber,country,state,
  to_date(dobdt,'yyyy-MM-dd') as dobdt,
  case when cast(datediff(curdt,dobdt)/365 as integer)=age then 'valid' else 'invalid' end as ageflag,curdt,curts 
  from (select 
  explode(results) as res,
  info.page as page,res.cell as cell,
  res.name.first as first,
  res.dob.age as age,
  res.dob.date as dobdt,
  res.email as email,res.location.city as uscity,res.location.coordinates.latitude as latitude,
  res.location.coordinates.longitude as longitude,res.location.country as country,
  res.location.state as state,
  res.location.timezone as timezone,res.login.username as username,current_date as curdt,current_timestamp as curts
 from apiview  
 ) as temp1
 where age>25
  """)
  esdf.printSchema();
  esdf.show(5,false)
//where info.page is not null
				println("Writing to Elastic Search")    
				esdf.saveToEs("sparkjson/custvisit",Map("es.mapping.id"->"custid"))

				dstream.asInstanceOf[CanCommitOffsets].commitAsync(offsetranges)
				println(java.time.LocalTime.now)
				println("commiting offset")
			}
			else
				{println(java.time.LocalTime.now)
				println("No data in the kafka topic for the given iteration")
				}
		
			}
			}
			
					catch {
						case ex1: java.lang.IllegalArgumentException => {
							println("Illegal arg exception")
						}

						case ex2: java.lang.ArrayIndexOutOfBoundsException => {
							println("Array index out of bound")
						}

						case ex3: org.apache.spark.SparkException => {
							println("Spark common exception")}
						
            case ex4: java.lang.NullPointerException => {
							println("Values Ignored")}
						    }
					
		ssc.start()
		ssc.awaitTermination()

	}
