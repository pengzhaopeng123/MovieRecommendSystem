package com.pengzhaopeng.statistics

import java.sql.Date
import java.text.SimpleDateFormat

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
  * 离线统计算法
  */
object StatisticsRecommender {

  val MONGODB_RATING_COLLECTION = "Rating"
  val MONGODB_MOVIE_COLLECTION = "Movie"

  //统计表的名称
  val RATE_MORE_MOVIES = "RateMoreMovies"
  val RATE_MORE_RECENTLY_MOVIES = "RateMoreRecentlyMovies"
  val AVERAGE_MOVIES = "AverageMovies"
  val GENRES_TOP_MOVIES = "GenresTopMovies"

  val MOVIE_TOP10_BY_SCORE = 10 //统计每种电影类型中评分最高的10个电影

  def main(args: Array[String]): Unit = {
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.db" -> "recommender"
    )

    val mongoAddress = "mongodb://192.168.2.4:27200,192.168.2.5:27200,192.168.2.6:27200/" + config("mongo.db") + "?readPreference=secondaryPreferred"

    //创建SparkConf配置
    val sparkConf = new SparkConf().setAppName("StatisticsRecommender").setMaster(config("spark.cores"))

    //创建SparkSession
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    import spark.implicits._

    //加载数据
    val ratingDF: DataFrame = spark
      .read
      .option("spark.mongodb.input.uri", mongoAddress)
      .option("collection", MONGODB_RATING_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[MovieRating]
      .toDF()

    val movieDF: DataFrame = spark
      .read
      .option("spark.mongodb.input.uri", mongoAddress)
      .option("collection", MONGODB_MOVIE_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[Movie]
      .toDF()

    //创建一张叫rating的表
    ratingDF.createOrReplaceTempView("ratings")

    // 案例1：统计所有历史数据中每个电影的评分个数
    //数据结构 -》  mid,count
    val rateMoreMoviesDF: DataFrame = spark.sql("select mid,count(mid) as count from ratings group by mid")

    rateMoreMoviesDF
      .write
      .option("spark.mongodb.output.uri", mongoAddress)
      .option("collection", RATE_MORE_MOVIES)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()


    // 案例2：统计以月为单位拟每个电影的评分个数
    //数据结构 -》 mid,count,time

    //创建一个日期格式化工具
    val simpleDateFormat = new SimpleDateFormat("yyyyMM")

    //注册一个UDF函数，用于将timestamp转换成年月格式 1260759144000  => 201605
    spark.udf.register("changeDate", (x: Int) => simpleDateFormat.format(new Date(x * 1000L)).toInt)

    //将原来的Rating数据集中的使劲啊转换成年月的格式
    val ratingOfYearMouth: DataFrame = spark.sql("select mid, score, changeDate(timestamp) as yearmouth from ratings")

    //将新的数据注册为一张表
    ratingOfYearMouth.createOrReplaceTempView("ratingOfMouth")

    //结算结果
    val rateMoreRecentlyMovies: DataFrame = spark.sql("select mid, count(mid) as count,yearmouth from ratingOfMouth group by yearmouth,mid")

    rateMoreRecentlyMovies
      .write
      .option("spark.mongodb.output.uri", mongoAddress)
      .option("collection", RATE_MORE_RECENTLY_MOVIES)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()


    // 案例3：统计每个电影的平均评分
    val averageMoviesDF: DataFrame = spark.sql("select mid, avg(score) as avg from ratings group by mid")

    averageMoviesDF
      .write
      .option("spark.mongodb.output.uri", mongoAddress)
      .option("collection", AVERAGE_MOVIES)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    // 案例4：统计每种电影类型中评分最高的10个电影
    // 案例4 第1步：需要通过Join操作将电影的平均评分数据和Movie数据进行合并，产生MovieWithScore数据集
    // 案例4 第2步：将电影的类别数据转换成RDD, GenresRDD
    // 案例4 第3步：将GenresRDD和MovieWithScore数据集进行笛卡尔积，产生一个 N * M 的数据集
    // 案例4 第4步：过滤掉电影中的真实类别和 GenresRDD中的类别不匹配的电影
    // 案例4 第5步：Genres作为key, groupByKey操作，将相同电影类别的电影进行聚合
    // 案例4 第6步：通过排序和提取 获取评分中最高的10个电影
    // 案例4 第7步：将结果输出到MongoDB中

    //具体步骤
    //应为只需要有评分的电影数据集
    val movieWithScore = movieDF.join(averageMoviesDF,Seq("mid","mid"))

    //测试
//    MovieWithScore.show()

    //TODO 所有电影类别 提取到配置文件
    val genresList = List("Action", "Adventure", "Animation", "Comedy", "Ccrime", "Documentary", "Drama", "Family", "Fantasy", "Foreign", "History", "Horror", "Music", "Mystery"
      , "Romance", "Science", "Tv", "Thriller", "War", "Western")

    //将电影类别转成RDD
    val genresRDD: RDD[String] = spark.sparkContext.makeRDD(genresList)

//    val genrenTopMovies: RDD[(String, Row)] = genresRDD.cartesian(movieWithScore.rdd)
//    genrenTopMovies.toDF().show()

    //计算电影类别top10
    val genrenTopMovies: DataFrame = genresRDD.cartesian(movieWithScore.rdd) //将电影类别和电影数据进行笛卡尔积操作
      .filter {
        //过滤掉电影的类别不匹配的电影
        case (genres, row) => row.getAs[String]("genres").toLowerCase.contains(genres.toLowerCase)
      }
      .map {
        //将整个数据集的数据量减小，生成的RDD[String,Iter[mid,avg]]
        case (genres, row) =>
          (genres, (row.getAs[Int]("mid"), row.getAs[Double]("avg")))
      }
      .groupByKey() //将genres数据集中的相同的聚焦
      .map {
        //通过评分的大小进行数据的排序 然后将数据映射成为对象
        case (genres, items) => GenresRecommendation(genres, items.toList.sortWith(_._2 > _._2).take(MOVIE_TOP10_BY_SCORE).map(x => Recommendation(x._1, x._2)))
      }
      .toDF()

    genrenTopMovies
      .write
      .option("spark.mongodb.output.uri", mongoAddress)
      .option("collection", GENRES_TOP_MOVIES)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    //关闭spark
    spark.stop()

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
  * 推荐对象
  *
  * @param rid 推荐的Movie的mid
  * @param r   Movie的评分
  */
case class Recommendation(rid: Int, r: Double)

/**
  * 统计每种电影类型中评分最高的10个电影  最终输出到mongodb中
  *
  * @param genres 电影的类别
  * @param recs   top10的电影的集合
  */
case class GenresRecommendation(genres: String, recs: Seq[Recommendation])
