import org.apache.spark.sql.DataFrame

object QueryConflictExit extends SparkSessionWrapper {
  def apply(conflicts: DataFrame, year: Int): DataFrame = {
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
         |  wiki
         |from event.twocolconflictexit
         |where year = ${year}
         |""".stripMargin
    )
  }
}
