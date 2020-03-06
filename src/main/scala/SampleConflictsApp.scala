import org.apache.spark.sql.functions.{concat, lit}

object SampleConflictsApp extends SparkSessionWrapper with Defines {
  def main(args: Array[String]): Unit = {
    import spark.implicits._

    val reader = spark.read
    val clean_conflicts = reader
      .format("parquet")
      .load(s"${DATA_DIR}/clean_conflicts")
      .cache

    clean_conflicts
      .withColumn("otherDiff", concat(
        lit("https://"),
        $"webhost",
        lit("/wiki/?oldid="),
        $"baseRevisionId",
        lit("&diff="),
        $"latestRevisionId"
      ) )
  }
}
