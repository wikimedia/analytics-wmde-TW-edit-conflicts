/**
 * Take a sample of edit conflict logs, and gather metadata about the conflicting revisions.
 */
object BuildConflictMetadataApp extends SparkSessionWrapper {
  def main(args: Array[String]): Unit = {
    val conflicts = DebugTable("conflicts", QueryConflictPeriod(year = 2020, month = 4))
    val conflict_rev_details = DebugTable("conflict_rev_details", QueryConflictRevisionMetadata(conflicts, 2020))

    QueryConflictExit(conflicts, 2020)
  }
}
