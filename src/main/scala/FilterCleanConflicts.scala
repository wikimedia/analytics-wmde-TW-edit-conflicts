import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.lit

object FilterCleanConflicts extends SparkSessionWrapper {
  def apply(conflicts: DataFrame): DataFrame = {
    import spark.implicits._

    conflicts
      // T246439 -- High proportion of edit conflicts seem to come from new article creation
      .filter($"base_rev_id" =!= lit(0))
      // T246440 -- High proportion of edit conflicts seem to not involve a conflicting edit
      .filter($"base_rev_id" =!= $"latest_rev_id")
  }
}
