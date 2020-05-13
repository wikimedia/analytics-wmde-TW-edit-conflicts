import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.unix_timestamp

object QueryLinkConflictExit extends SparkSessionWrapper {
  def apply(conflicts: DataFrame, exits: DataFrame): DataFrame = {
    import spark.implicits._

    conflicts
      .join(
        exits,
        conflicts("wiki") === exits("wiki")
          and conflicts("page_namespace") === exits("page_namespace")
          and conflicts("page_title") === exits("page_title")
          and ((conflicts("base_rev_id") === 0) or (conflicts("base_rev_id") === exits("base_rev_id")))
          and (conflicts("start_time_ts_s").cast("bigint") * 1000 === exits("start_time_ts_ms").cast("bigint")),
        "inner"
      )
      .select(
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
  }
}
