package com.pengzhaopeng

import java.net.InetAddress

import com.mongodb.casbah.Imports.ServerAddress
import com.mongodb.casbah.MongoClient
import com.mongodb.casbah.commons.MongoDBObject
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.{InetSocketTransportAddress, TransportAddress}
import org.elasticsearch.transport.client.PreBuiltTransportClient
import org.elasticsearch.spark.sql._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SQLContext._

/**
  * 数据加载服务
  */
object DataLoader {

  //源文件 电影 评分 标签
  val MOVIE_DATA_PATH = "D:\\IDEAProject\\MovieRecommendSystem\\recommender\\dataloader\\src\\main\\resources\\movies.csv"
  val RATING_DATA_PATH = "D:\\IDEAProject\\MovieRecommendSystem\\recommender\\dataloader\\src\\main\\resources\\ratings.csv"
  val TAG_DATA_PATH = "D:\\IDEAProject\\MovieRecommendSystem\\recommender\\dataloader\\src\\main\\resources\\tags.csv"

  //collection
  val MONGODB_MOVIE_COLLECTION = "Movie"
  val MONGODB_RATING_COLLECTION = "Rating"
  val MONGODB_TAG_COLLECTION = "Tag"

  val mongoIp1 = "192.168.2.4"
  val mongoIp2 = "192.168.2.5"
  val mongoIp3 = "192.168.2.6"
  val mongoPort = 27200

  //ES index
  val ES_MOVE_INDEX = "Movie"


  def main(args: Array[String]): Unit = {

    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.db" -> "recommender",
      "es.httpHosts" -> "hadoop01:9200,hadoop02:9200,hadoop03:9200",
      "es.transportHosts" -> "hadoop01:9300,hadoop02:9300,hadoop03:9300",
      "es.index" -> "recommender",
      "es.cluster.name" -> "bigdata"
    )

    val mongoAddress = "mongodb://192.168.2.4:27200,192.168.2.5:27200,192.168.2.6:27200/" + config("mongo.db")


    //需要创建一个SparkConf配置
    val sparkConf: SparkConf = new SparkConf()
      .setAppName("DataLoader")
      .setMaster(config("spark.cores"))

    //创建一个SparkSession
    val spark: SparkSession = SparkSession
      .builder()
      .config(sparkConf)
      .getOrCreate()

    import spark.implicits._

    //将Movie数据集加载进来
    val movieRDD: RDD[String] = spark.sparkContext.textFile(MOVIE_DATA_PATH)
    //将MovieRDD转换成DataFrame
    val movieDF: DataFrame = movieRDD.map(item => {
      val attr: Array[String] = item.split("\\^")
      Movie(attr(0).toInt, attr(1).trim, attr(2).trim, attr(3).trim, attr(4).trim,
        attr(5).trim, attr(6).trim, attr(7).trim, attr(8).trim, attr(9).trim)
    }).toDF()

    //将Rating数据集加载进来
    val ratingRDD: RDD[String] = spark.sparkContext.textFile(RATING_DATA_PATH)
    //将ratingRDD转换成DataFrame
    val ratingDF: DataFrame = ratingRDD.map(item => {
      val attr: Array[String] = item.split(",")
      Rating(attr(0).toInt, attr(1).toInt, attr(2).toDouble, attr(3).toInt)
    }).toDF()

    //将Rating数据集加载进来
    val tagRDD: RDD[String] = spark.sparkContext.textFile(TAG_DATA_PATH)
    //将tagRDD转换成DataFrame
    val tagDF: DataFrame = tagRDD.map(item => {
      val attr: Array[String] = item.split(",")
      Tag(attr(0).toInt, attr(1).toInt, attr(2).trim, attr(3).toInt)
    }).toDF()

    //定义连接mongodb的隐士配置 方便传参
    val addressesList = List(new ServerAddress(mongoIp1, mongoPort), new ServerAddress(mongoIp1, mongoPort), new ServerAddress(mongoIp1, mongoPort))

    implicit val mongoConfig = MongoConfig(addressesList, config("mongo.db"))

    //将数据保存到MongoDB中
    //    storeDataInMongoDB(movieDF, ratingDF, tagDF,mongoAddress)

    //将数据存入ES前进行预处理 处理后的形式为 MID, tag1|tag2|tag3
    /**
      * MID , Tags
      * 1     tag1|tag2|tag3|tag4....
      */
    import org.apache.spark.sql.functions._
    val newTag: DataFrame = tagDF.groupBy($"mid").agg(concat_ws("|", collect_set($"tag")).as("tags")).select("mid", "tags")

    //需要将处理后的Tag数据 和 Movie数据融合 产生新的 Moive 数据
    val movieWithTagsDF: DataFrame = movieDF.join(newTag, Seq("mid"), "left")


    //声明了一个ES配置的隐式参数
    implicit val esConfig = ESConfig(config("es.httpHosts"), config("es.transportHosts"), config("es.index"), config("es.cluster.name"))

    //将新的数据保存到ES中
    movieWithTagsDF.show()
//    storeDataInES(movieWithTagsDF);

    //关闭spark
    spark.stop()
  }

  /**
    * 将数据保存到ES中
    *
    * @param movieWithTagsDF
    * @return
    */
  def storeDataInES(movieWithTagsDF: DataFrame)(implicit esConfig: ESConfig) = {

    //新建一个配置 配置集群名称别忘了
    val settings: Settings = Settings.builder().put("cluster.name", esConfig.clusterName).build()

    //新建一个ES客户端
    val esClient = new PreBuiltTransportClient(settings)

    //需要将TransportHosts添加到esClient中
    //正则表达式： 任意字符（一到多个）: 任意数字（一到多个）    hadoop01:9200
    val REGEX_HOST_PORT = "(.+):(\\d+)".r
    esConfig.transportHosts.split(",").foreach {
      case REGEX_HOST_PORT(host: String, port: String) => {
        esClient.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(host), port.toInt))
      }
    }

//    esClient.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("192.168.2.4"), 9300))
//    esClient.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("192.168.2.5"), 9300))
//    esClient.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("192.168.2.6"), 9300))


    //清除ES中遗留的数据
    if (esClient.admin().indices().exists(new IndicesExistsRequest(esConfig.index)).actionGet().isExists) {
      esClient.admin().indices().delete(new DeleteIndexRequest(esConfig.index))
    }
    //创建index(库)
    esClient.admin().indices().create(new CreateIndexRequest(esConfig.index))

    //将数据写入到ES中


    movieWithTagsDF
      .write
      .option("es.nodes", esConfig.httpHosts)
      .option("es.http.timeout", "100m")
      .option("es.mapping.id", "mid")
      .mode("overwrite")
      .format("org.elasticsearch.spark.sql")
      .save(esConfig.index + "/" + ES_MOVE_INDEX)

  }

  /**
    * 将数据保存到MongoDB中
    */
  def storeDataInMongoDB(movieDF: DataFrame, ratingDF: DataFrame, tagDF: DataFrame, mongoAddress: String)(implicit mongoConfig: MongoConfig): Unit = {

    //新建一个MongoDB的连接 TODO 生产环境配置成连接池 https://www.cnblogs.com/jycboy/p/10077080.html
    val mongoClient = MongoClient(mongoConfig.listAddress)

    //如果MongoDB中有对应的数据库，那么应该删除
    mongoClient(mongoConfig.db)(MONGODB_MOVIE_COLLECTION).dropCollection()
    mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION).dropCollection()
    mongoClient(mongoConfig.db)(MONGODB_TAG_COLLECTION).dropCollection()

    //将当前数据写入到MongoDB
    movieDF
      .write
      .option("spark.mongodb.output.uri", mongoAddress)
      .option("collection", MONGODB_MOVIE_COLLECTION)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    ratingDF
      .write
      .option("spark.mongodb.output.uri", mongoAddress)
      .option("collection", MONGODB_RATING_COLLECTION)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    tagDF
      .write
      .option("spark.mongodb.output.uri", mongoAddress)
      .option("collection", MONGODB_TAG_COLLECTION)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    //对数据表建立索引
    mongoClient(mongoConfig.db)(MONGODB_MOVIE_COLLECTION).createIndex(MongoDBObject("mid" -> 1))
    mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION).createIndex(MongoDBObject("uid" -> 1))
    mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION).createIndex(MongoDBObject("mid" -> 1))
    mongoClient(mongoConfig.db)(MONGODB_TAG_COLLECTION).createIndex(MongoDBObject("uid" -> 1))
    mongoClient(mongoConfig.db)(MONGODB_TAG_COLLECTION).createIndex(MongoDBObject("mid" -> 1))

    //关闭MongoDB的连接
    mongoClient.close()
  }
}


/**
  * MongoDB的连接配置
  *
  * @param listAddress 集群地址
  * @param db          数据库
  */
case class MongoConfig(listAddress: List[ServerAddress], db: String)

/**
  *
  * ElasticSearch的连接配置
  *
  * @param httpHosts      Http的主机列表，以，分割
  * @param transportHosts Transport主机列表， 以，分割
  * @param index          需要操作的索引
  */
case class ESConfig(httpHosts: String, transportHosts: String, index: String, clusterName: String)

/**
  * Movie数据集，数据集字段通过分割
  *
  * 151^                          电影的ID
  * Rob Roy (1995)^               电影的名称
  * In the highlands ....^        电影的描述
  * 139 minutes^                  电影的时长
  * August 26, 1997^              电影的发行日期
  * 1995^                         电影的拍摄日期
  * English ^                     电影的语言
  * Action|Drama|Romance|War ^    电影的类型
  * Liam Neeson|Jessica Lange...  电影的演员
  * Michael Caton-Jones           电影的导演
  *
  * tag1|tag2|tag3|....           电影的Tag
  **/
case class Movie(mid: Int, name: String, descri: String, timelong: String, issue: String,
                 shoot: String, language: String, genres: String, actors: String, directors: String)

/**
  * Rating数据集，用户对于电影的评分数据集，用，分割
  *
  * 1,           用户的ID
  * 31,          电影的ID
  * 2.5,         用户对于电影的评分
  * 1260759144   用户对于电影评分的时间
  */
case class Rating(uid: Int, mid: Int, score: Double, timestamp: Int)

/**
  * Tag数据集，用户对于电影的标签数据集，用，分割
  *
  * 15,          用户的ID
  * 1955,        电影的ID
  * dentist,     标签的具体内容
  * 1193435061   用户对于电影打标签的时间
  */
case class Tag(uid: Int, mid: Int, tag: String, timestamp: Int)