import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object QueryConflictExit extends SparkSessionWrapper {
  def apply(conflicts: DataFrame, year: Int): Unit = {
    import spark.implicits._

    val exits = DebugTable("exits",
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
    )

    val linked_exits = DebugTable("linked_exits",
      conflicts
        .join(
          exits,
          conflicts("wiki") === exits("wiki")
            and conflicts("page_namespace") === exits("page_namespace")
            and conflicts("page_title") === exits("page_title")
            and ((conflicts("base_rev_id") === 0) or (conflicts("base_rev_id") === exits("base_rev_id")))
            and (conflicts("start_time_ts_s").cast("bigint") * 1000 === exits("start_time_ts_ms").cast("bigint")),
          "inner"
        ).select(
          conflicts("*"),
          exits("exit_action"),
          exits("exit_timestamp")
        )
        .withColumn("elapsed_s",
          // FIXME: Maybe eventlogging timestamps don't work this way?  There
          //  are lots of negative numbers, which makes me think the `event.*.dt`
          //  column might be warped by server event processing.
          unix_timestamp($"exit_timestamp") - unix_timestamp($"conflict_timestamp")
        )
    )
  }


}
