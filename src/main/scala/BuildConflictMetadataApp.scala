import org.apache.spark.sql.SaveMode;

/**
 * Take a sample of edit conflict logs, and gather metadata about the conflicting revisions.
 */
object BuildConflictMetadataApp extends SparkSessionWrapper {
  def main(args: Array[String]): Unit = {

    val conflicts = DebugTable("conflicts", QueryConflictPeriod(2020, 2))
    DebugTable("conflict_details", QueryConflictMetadata(conflicts, 2020))

    // FIXME: Is this necessary?
    spark.stop()
  }
}
