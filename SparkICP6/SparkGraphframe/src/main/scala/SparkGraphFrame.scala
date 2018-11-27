import org.apache.spark.sql.{DataFrame, SparkSession}
import org.graphframes.GraphFrame

object SparkGraphFrame {
  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "C:\\winutils")
    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config("spark.master", "local")
      .getOrCreate()

    /*
    val input = spark.createDataFrame(List(
      ("a", "Alice", 34),
      ("b", "Bob", 36),
      ("c", "Charlie", 30),
      ("d", "David", 29),
      ("e", "Esther", 32),
      ("f", "Fanny", 36),
      ("g", "Gabby", 60)
    )).toDF("id", "name", "age")
    val output = spark.createDataFrame(List(
      ("a", "b", "friend"),
      ("b", "c", "follow"),
      ("c", "b", "follow"),
      ("f", "c", "follow"),
      ("e", "f", "follow"),
      ("e", "d", "friend"),
      ("d", "a", "friend"),
      ("a", "e", "friend")
    )).toDF("src", "dst", "relationship")



    //6
    val verticesG = spark.read
      .options(Map("header" -> "true", "delimiter" -> "\t"))
      .csv("C:\\Users\\steph\\Downloads\\ConsumerComplaints.csv")
      .toDF("Product Name", "Sub Product", "Issue", "Sub Issue"
        , "Consumer Complaint Narrative", "Company Public Response", "Company","id")

    val edgesG = spark.read
      .options(Map("header" -> "true", "delimiter" -> ","))
      .csv("C:\\Users\\steph\\Downloads\\ConsumerComplaints (1).csv")
      .toDF("src", "State Name", "Zip Code", "Tags", "Consumer Consent Provided",
        "Submitted via", "Date Sent to Company", "Company Response to Consumer",
        "Timely Response", "Consumer Disputed", "dst")

    //1
    val maybeGraph = GraphFrame(verticesG,edgesG)

    val g = GraphFrame(input,output)
    g.vertices.show()
    g.edges.show()

    //2 concatenate two columns together
    //It's been a long time and I still have no idea how to do this
    //I looked up online and could find nothing on concatenating graphs

    //3 remove duplicates and then create the graph
*/
    /*
    val noDup = GraphFrame(verticesG.dropDuplicates(),edgesG.dropDuplicates())

    noDup.vertices.show()
    noDup.edges.show()

    //4 I think I already did this on lines 37 and 43
    //if I type something else instead of "Product Name"
    // then that is the column's name

    println("DATAFRAME!!!!!!!!!!!!!!")

    //5
    edgesG.show()
    verticesG.show()

    println("GRAPH!!!!!!!!!!!!!!!!!!!")

    //7
    maybeGraph.vertices.show()
    //8
    maybeGraph.edges.show()

    //9
    maybeGraph.inDegrees.show()
    //10
    maybeGraph.outDegrees.show()

    //degrees
    maybeGraph.degrees.show()

    */

    //ICP 6 Spark

    val vertG = spark.read
      .options(Map("header" -> "true", "delimiter" -> ","))
      .csv("C:\\Users\\steph\\Downloads\\ConsumerComplaints.csv")
      .toDF("Product_Name", "Sub_Product", "Issue", "Sub_Issue"
        , "Consumer_Complaint_Narrative", "Company_Public_Response", "Company","id")

    val edG = spark.read
      .options(Map("header" -> "true", "delimiter" -> ","))
      .csv("C:\\Users\\steph\\Downloads\\ConsumerComplaints (1).csv")
      .toDF("src", "State_Name", "Zip_Code", "Tags", "Consumer_Consent_Provided",
        "Submitted_via", "Date_Sent_to_Company", "Company_Response_to_Consumer",
        "Timely_Response", "Consumer_Disputed", "dst")


    val icp6Graph = GraphFrame(vertG,edG)

    //BFS

    val paths: DataFrame = icp6Graph.bfs.fromExpr("Issue = 'Other'")
      .toExpr("id < 48032").run()

    paths.show()

    //Page Rank

    val results = icp6Graph.pageRank.resetProbability(0.15).tol(0.01).run()
    results.vertices.select("id","pagerank").show()
    results.edges.select("src","dst","weight").show()

    //Triangle Count

    val triCount = icp6Graph.triangleCount.run()
    triCount.select("id","count").show()

    //Save Graph

    vertG.write
      .parquet("C:\\Users\\steph\\Downloads\\SparkGraphframe\\SparkGraphframe\\src\\main\\scala\\myVertices.parquet")

    edG.write
      .parquet("C:\\Users\\steph\\Downloads\\SparkGraphframe\\SparkGraphframe\\src\\main\\scala\\myEdges.parquet")


  }
}
