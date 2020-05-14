import org.apache.spark.sql.DataFrame

object BuildCleanConflicts extends SparkSessionWrapper {
  def apply(conflict_details: DataFrame): DataFrame = {
    import spark.implicits._

    val total_rows = conflict_details.count

    var cleaned_conflicts = conflict_details.dropDuplicates
    println(s"Removed ${total_rows - cleaned_conflicts.count} duplicate rows.")

    val total_seems_new = conflict_details.filter($"baseRevisionId" === 0).count
    println("T246439 -- High proportion of edit conflicts seem to come from new article creation:")
    println(s"  ${total_seems_new} rows")

    val total_seems_nonconflicting = conflict_details.filter($"baseRevisionId" === $"latestRevisionId" and ($"baseRevisionId" =!= 0)).count
    println("T246440 -- High proportion of edit conflicts seem to not involve a conflicting edit:")
    println(s"  ${total_seems_nonconflicting} rows")

    val clean_conflicts = conflict_details.dropDuplicates.filter($"baseRevisionId" =!= $"latestRevisionId").cache

    println(s"Remaining conflicts without known issues: ${clean_conflicts.count}")
    clean_conflicts
  }
}
