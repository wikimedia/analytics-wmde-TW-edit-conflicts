import org.apache.spark.sql.SaveMode;

/**
 * Take a sample of edit conflict logs, and gather metadata about the conflicting revisions.
 */
object BuildConflictMetadataApp extends SparkSessionWrapper {
  def main(args: Array[String]): Unit = {
    val conflicts = DebugTable("conflicts", QueryConflictPeriod(2020, 3))
    val conflict_rev_details = DebugTable("conflict_rev_details", QueryConflictRevisionMetadata(conflicts, 2020))
    val edit_attempt_steps = DebugTable("conflict_edit_steps", QueryConflictEditAttemptSteps(conflicts, 2020))
    //val conflict_workflows = DebugTable("conflict_workflows", SummarizeConflictSteps(edit_attempt_steps))

    // FIXME: Is this necessary?
    spark.stop()
  }
}
