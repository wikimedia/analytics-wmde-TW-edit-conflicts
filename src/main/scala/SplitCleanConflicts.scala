import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.lit

// TODO: Do this efficiently; as a chain of splits?
object SplitCleanConflicts extends SparkSessionWrapper {
  def apply(conflicts: DataFrame): (DataFrame, DataFrame, DataFrame) = {
    import spark.implicits._

    // T246439 -- High proportion of edit conflicts seem to come from new article creation
    val new_article_conflicts = PersistentTable.refresh(
      name = "new_article_conflicts",
      calculate = () =>
        conflicts.filter($"base_rev_id" === lit(0))
    )

    // T246440 -- High proportion of edit conflicts seem to not involve a conflicting edit
    val no_conflict_conflicts = PersistentTable.refresh(
      name = "no_conflict_conflicts",
      calculate = () =>
        conflicts.filter(
          $"base_rev_id" === $"latest_rev_id"
            && $"base_rev_id" =!= lit(0))
    )

    val clean_conflicts = PersistentTable.refresh(
      name = "clean_conflicts",
      calculate = () =>
        conflicts
          .filter($"base_rev_id" =!= lit(0))
          .filter($"base_rev_id" =!= $"latest_rev_id")
    ).as("conflicts").cache()

    (clean_conflicts, new_article_conflicts, no_conflict_conflicts)
  }
}
