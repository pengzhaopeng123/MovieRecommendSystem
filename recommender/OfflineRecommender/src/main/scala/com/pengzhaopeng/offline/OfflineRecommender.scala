package com.pengzhaopeng.offline

import org.apache.spark.SparkConf
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * 电影离线推荐算法 计算用户推荐电影
  */
object OfflineRecommender {

  //TODO 配置文件待封装
  val USER_MAX_RECOMMENDATION = 10 //推荐给用户的电影个数
  val MONGODB_MOVIE_COLLECTION = "Movie"
  val MONGODB_RATING_COLLECTION = "Rating"
  val USER_RECS = "UserRecs" //最后统计完写入到mongdb的库

  //  val mongoIp1 = "192.168.2.4"
  //  val mongoIp2 = "192.168.2.5"
  //  val mongoIp3 = "192.168.2.6"
  //  val mongoPort = 27200

  //入口方法
  def main(args: Array[String]): Unit = {

    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.db" -> "recommender"
    )

    val mongoAddress = "mongodb://192.168.2.4:27200,192.168.2.5:27200,192.168.2.6:27200/" + config("mongo.db") + "?readPreference=secondaryPreferred"

    // 第1步：基于SparkCong创建一个SparkSession
    //部署参数 参考 http://spark.apache.org/docs/2.2.0/configuration.html
    val sparkConf = new SparkConf()
      .setAppName("OfflineRecommender")
      .setMaster(config("spark.cores"))
      .set("spark.executor.memory", "6G")
      .set("spark.driver.memory", "2G")
    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    import spark.implicits._

    // 第2步：读取MongoDB中的业务数据 (rating)
    val ratingRDD = spark
      .read
      .option("spark.mongodb.input.uri", mongoAddress)
      .option("collection", MONGODB_RATING_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[MovieRating]
      .rdd
      .map(rating => (rating.uid, rating.mid, rating.score))

    // ALS模型 第一个参数 ---训练数据集
    val trainData: RDD[Rating] = ratingRDD.map(x => Rating(x._1, x._2, x._3))
    //ALS模型 其他参数
    // rank -- 用户特征数
    // iterations -- 迭代次数
    // lamba -- 正则化参数 越小越好 参考资料文档
    val (rank, iterations, lambda) = (50, 5, 0.01)

    // 第3步： 训练ALS模型
    val model: MatrixFactorizationModel = ALS.train(trainData, rank, iterations, lambda)

    //计算用户推荐电影矩阵参数：需要构造一个userProducts RDD[(Int, Int)]
    //用户的数据集 RDD[Int]
    val userRDD: RDD[Int] = ratingRDD.map(_._1).distinct()
    //电影的数据集 RDD[Int]
    val movieRDD = spark
      .read
      .option("spark.mongodb.input.uri", mongoAddress)
      .option("collection", MONGODB_MOVIE_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[Movie]
      .rdd
      .map(_.mid)
    //构造userProducts RDD[(Int, Int)]
    val userMovies: RDD[(Int, Int)] = userRDD.cartesian(movieRDD)

    // 第4步：计算用户推荐电影矩阵
    val preRatings: RDD[Rating] = model.predict(userMovies)

    //处理过滤数据再写入Mongodb 预设结果： (UID: [(mid,sorce),(mid,sorce).....])
    val userRecs: DataFrame = preRatings
      .filter(_.rating > 0)
      .map(rating => (rating.user, (rating.product, rating.rating)))
      .groupByKey()
      .map {
        case (uid, recs) => UserRecs(uid, recs.toList.sortWith(_._2 > _._2).take(USER_MAX_RECOMMENDATION).map(x => Recommendation(x._1, x._2)))
      }.toDF()

    // 第5步：将计算完的结果写入到MongoDB中
    userRecs.write
      .option("spark.mongodb.output.uri", mongoAddress)
      .option("collection", USER_RECS)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    // 第6步：关闭Spark
    spark.close()

  }

}

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
  * @param uid       用户的ID
  * @param mid       电影的ID
  * @param score     用户对于电影的评分
  * @param timestamp 用户对于电影评分的时间
  */
case class MovieRating(uid: Int, mid: Int, score: Double, timestamp: Int)

/**
  * 用户的推荐
  *
  * @param mid   电影id
  * @param score 电影评分
  */
case class Recommendation(mid: Int, score: Double)

/**
  * 用户的推荐 最终的格式 输入到mongodb中
  *
  * @param uid  用户id
  * @param recs 用户的推荐
  */
case class UserRecs(uid: Int, recs: Seq[Recommendation])
