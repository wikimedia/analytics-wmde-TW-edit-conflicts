import java.sql.Timestamp

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object SummarizeConflictSteps {
  def apply(conflicts: DataFrame, conflict_edit_steps: DataFrame): DataFrame = {
    // TODO: smaller coupling
    val trackable_sessions = DebugTable.loadIntermediateTable("trackable_sessions")

    val sessionWindow = Window
      .partitionBy("editing_session_id")
        .orderBy("step_timestamp")

    conflicts
        .join(
          trackable_sessions,
          conflicts(
        )
        .withColumn("elapsed_time", max("step_timestamp") - min("step_timestamp"))
    conflict_edit_steps

    // TODO:
    // join back through tracked_sessions to conflicts
    //  * Add columns to conflicts
    //  * editing_session_id
    //  * isSuccess: was there a saveSuccess line with step_rev_id > saveAttempt id?
    //  * editor_interface
    //  * interface_user_id
    //  * interface_user_edit_count
    //  * abort, abort_type, abort_mechanism
    //  * join all actions
    //  * is_oversample


  }

  def elapsedTime(times: Seq[Timestamp]): Float = {
    (times.last.getTime - times.head.getTime) / 1000;
  }
}
