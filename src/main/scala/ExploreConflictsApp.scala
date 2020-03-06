import org.apache.spark.sql.functions.count

object ExploreConflictsApp extends SparkSessionWrapper with Defines {
  def main(args: Array[String]): Unit = {
    import spark.implicits._

    val reader = spark.read
    val conflict_details = reader
      .format("parquet")
      .load(s"${DATA_DIR}/conflict_details")
      .cache

    println("Conflict details schema:")
    conflict_details.schema.printTreeString

    val clean_conflicts = DebugTable(
      "clean_conflicts",
      BuildCleanConflicts(conflict_details)
    )

    println("Which namespaces experience the most conflicts? (nulls are caused by limiting the window for joining against the base revision)")
    clean_conflicts
      .groupBy("page_namespace")
      .agg(count("*")
        .as("count"))
      .sort($"count".desc)
      .show

    println("Which wikis experience the most conflicts?")
    clean_conflicts
      .groupBy("wiki")
      .agg(count("*")
        .as("count"))
      .sort($"count".desc)
      .show

    println("What's happening on commonswiki?")
    clean_conflicts
      .filter($"wiki" === "commonswiki")
      .groupBy("page_namespace")
      .agg(count("*")
      .as("count"))
      .sort($"count".desc)
      .show

    println("Are there any pages which are hotspots for conflicts?")
    clean_conflicts
      .filter($"page_id".isNotNull)
      .groupBy("wiki", "page_title")
      .agg(count("*")
        .as("count"))
      .sort($"count".desc)
      .show(false)

    println("Suspected self-conflicts: next edit is made by the same user as the conflicting edit.")
    println(s"  ${clean_conflicts.filter($"other_user" === $"next_user").count} rows")

    // FIXME: Is this necessary?
    spark.stop()
  }
}
