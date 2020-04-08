import org.apache.spark.sql.DataFrame

object QueryConflictPeriod extends SparkSessionWrapper {
  def apply(year: Int, month: Int): DataFrame = {
    spark.sql(
      s"""
        |select
        |  to_timestamp(dt) as conflict_timestamp,
        |  wiki,
        |  event.baseRevisionId as base_rev_id,
        |  event.latestRevisionId as latest_rev_id,
        |  event.isAnon as user_is_anon,
        |  event.editCount as user_editcount,
        |  event.twoColConflictShown as is_twocol,
        |  event.hasJavascript as is_js,
        |  event.pageNs as page_namespace,
        |  event.pageTitle as page_title,
        |  event.startTime as start_time_ts_s,
        |  event.conflictChunks as conflict_chunks,
        |  event.conflictChars as conflict_chars
        |from event.twocolconflictconflict
        |where year = ${year} and month = ${month}
        |""".stripMargin
    ).cache
  }
}
