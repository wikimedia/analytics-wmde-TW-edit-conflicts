import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{lit, size, split, substring, when}

object QueryConflictExit extends SparkSessionWrapper {
  def apply(conflicts: DataFrame, year: Int): DataFrame = {
    import spark.implicits._

    spark.sql(
      s"""
         |select
         |  to_timestamp(dt) as exit_timestamp,
         |  event.start_time_ts_ms,
         |  event.action as exit_action,
         |  event.base_rev_id,
         |  event.latest_rev_id,
         |  event.page_namespace,
         |  event.page_title,
         |  event.selections,
         |  event.session_token,
         |  wiki
         |from event.twocolconflictexit
         |where year = ${year}
         |""".stripMargin
    )
    .withColumn("v1_selections",
      when(substring($"selections", 1, 3) === lit("v1:"), $"selections")
        .otherwise(lit(null)))
    .withColumn("row_count",
      size(split($"v1_selections", "\\|")))
  }
}
