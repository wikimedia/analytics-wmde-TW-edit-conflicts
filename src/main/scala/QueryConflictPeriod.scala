import org.apache.spark.sql.DataFrame

object QueryConflictPeriod extends SparkSessionWrapper {
  def apply(year: Int, month: Int): DataFrame = {
    spark.sql(
      s"""
        |select
        |  to_timestamp(dt) as conflict_timestamp,
        |  webhost,
        |  wiki,
        |  event.baseRevisionId,
        |  event.latestRevisionId,
        |  event.editCount,
        |  replace(event.textUser, '\n', '\\n') as textbox,
        |  useragent.browser_family,
        |  useragent.os_family
        |from event.twocolconflictconflict
        |where year = ${year} and month = ${month}
        |""".stripMargin
    ).cache
  }
}
