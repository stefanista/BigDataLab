import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame

object SparkDataFrame {
  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "C:\\winutils")
    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config("spark.master", "local")
      .getOrCreate()

    /*

    val df = spark.read.format("csv").option("header","true")
      .load("C:\\Users\\steph\\Downloads\\SparkDataframe1\\SparkDataframe\\src\\main\\scala\\people.csv")

    val df2 = spark.read.format("csv").option("header","true")
      .load("C:\\Users\\steph\\Downloads\\SparkDataframe1\\SparkDataframe\\src\\main\\scala\\people2.csv")


    df.show()

    print("Saving to file now")
    //df.write.parquet("C:\\Users\\steph\\Downloads\\SparkDataframe1\\SparkDataframe\\src\\main\\scala\\myFile1.parquet")

    print("Count the number of repeated records in the dataset")
    print("duplicat count: " )
    print(df.count()- df.dropDuplicates().count())

    print("Apply Union to dataset and outputting the Company alphabetically")

    //df.printSchema()
    val unionDF = df.union(df2)
    val sortDF = df.orderBy("Company")

    unionDF.show()
    sortDF.show()

    print("Group by Zip codes")

    val groupDF = df.groupBy("Zip Code").count().show()

    print("Join 1")

    val joinedDF = df.join(df2, "Company")
    joinedDF.show()

    print("Aggregate 1")

    val aggDF = df.groupBy("Complaint ID").sum().show()

    print("Fetching 13th row")

    val rowThirteen = df.rdd.take(13).last
    println(rowThirteen)

    */

    //Lab Below Here

    //reading a csv format, it has a header, the struct types are going to be inferred, and the file path is specified
    val df = spark.read.format("csv").option("header","true").option("inferSchema", true)
      .load("C:\\Users\\steph\\Downloads\\SparkDataframe1\\SparkDataframe\\src\\main\\scala\\WorldCupMatches.csv")


    //used to check if it is printing
    df.show()

    //dataframe q1 gorup by the city and get max attendance

    val dataf1 = df.groupBy("City").max("Attendance").show()

    //dataframe q2 gets a dataframe with high scoring matches, another with high attendance and joins them on the home team
    val dataf2a = df.filter(df("Home Team Goals") > 3 && df("Away Team Goals") > 3)
    val dataf2b = df.filter(df("Attendance") > 4000)

    val joinDf = dataf2a.join(dataf2b, dataf2a("Home Team Name") === dataf2b("Home Team Name")).distinct()
    joinDf.show()


    //dataframe q3 total home goals home teams had
    val dataf3 = df.groupBy("Home Team Name").sum("Home Team Goals").show()


    //dataframe q4 how many times did a home team have more than 5 goals
    val dataf4 = df.filter(df("Home Team Goals") > 4 ).groupBy("Home Team Name").count().show()


    //dataframe q5 by year England's total home and away goals
    val dataf5a = df.filter(df("Home Team Name") === "England" || df("Away Team Name") === "England")
      .groupBy("Year").sum("Home Team Goals", "Away Team Goals").show()



    //RDD query 1 get a specific stadium
    val filterStadium = df.rdd.filter(line => line(3) == "Pocitos")

    filterStadium.collect().foreach(println)

    //RDD query 2 get france and mexico matches when France was home
    val filterMatch = df.rdd.filter(l => l(5) == "France" & l(8) == "Mexico")

    filterMatch.collect().foreach(println)

    //RDD query 3 any USA game and sorted by most recent year
    val filterUSA = df.rdd.filter(e => e(5) == "USA" || e(8) == "USA")
      .sortBy(line => line(0).toString, false)
    filterUSA.collect().foreach(println)

    //RDD query 4 group by year
    val groupYear = df.rdd.groupBy(e => e(0) )

    groupYear.collect().foreach(println)
   // println(groupYear.count())

    //RDD query 5 group by home team and count how many teams
    val groupTeam = df.rdd.filter(n => n(0) != null).groupBy(e => e(5))

    groupTeam.collect().foreach(println)
    println(groupTeam.count())

    //dataframe pair for rdd query 1
    val q1df = df.filter(df("Stadium")=== "Pocitos").show()

    //dataframe pair for rdd query 2
    val q2df = df.filter(df("Home Team Name")=== "France" && df("Away Team Name")=== "Mexico" ).show()

    //dataframe pair for rdd query 3
    val q3df = df.filter(df("Home Team Name")=== "USA" || df("Away Team Name")=== "USA")
      .orderBy(org.apache.spark.sql.functions.col("Year").desc).show()

    //dataframe pair for rdd query 4 count is required
    val q4DF = df.groupBy("Year").count().show()

    //dataframe pair for rdd query 5 count does not give you the number of each team
    val q5DF = df.groupBy("Home Team Name").count().show()




  }

}
