import org.apache.spark.sql.{SaveMode, SparkSession}

object ConflictApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("Conflict Query").getOrCreate()
    val conflicts = spark.sql("""
              |select
              |   webhost,
              |   wiki,
              |   event.baseRevisionId,
              |   event.latestRevisionId,
              |   event.editCount,
              |   replace(event.textUser, '\n', '\\n')
              |   useragent.browser_family,
              |   useragent.os_family
              |from event.twocolconflictconflict
              |where year = 2020 and month = 2
              |""".stripMargin
        )
    val base_revs = 
    conflicts
      .coalesce(1)
      .write
      .option("header", "true")
      .mode(SaveMode.Overwrite)
      .csv("/tmp/awight/conflicts")
    spark.stop()
  }
}
