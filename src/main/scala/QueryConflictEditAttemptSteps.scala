import org.apache.spark.sql.DataFrame

object QueryConflictEditAttemptSteps extends SparkSessionWrapper {
  def apply(conflicts: DataFrame, year: Int): DataFrame = {
    import spark.implicits._

    val edit_steps = spark.sql(
      s"""
         |select
         |  to_timestamp(dt) as step_timestamp,
         |  event.action,
         |  event.abort_mechanism,
         |  event.abort_type,
         |  event.is_oversample,
         |  event.editing_session_id,
         |  event.editor_interface,
         |  event.user_id,
         |  event.user_editcount,
         |  event.revision_id as step_rev_id,
         |  wiki
         |from event.editattemptstep
         |where year = ${year}
         |""".stripMargin
    )

    val edit_attempt_step = spark.sql(
      s"""
         |select
         |  event.editing_session_id as tracked_editing_session_id,
         |  wiki,
         |  event.page_ns,
         |  event.page_title
         |from event.editattemptstep
         |where year = ${year}
         |""".stripMargin
    )
    val tracked_conflicts =
      conflicts
        .join(
          edit_attempt_step,
          conflicts("wiki") === edit_attempt_step("wiki")
            and conflicts("page_namespace") === edit_attempt_step("page_ns")
            and conflicts("page_title") === edit_attempt_step("page_title"),
          "left"
        ).select(
          $"conflicts.*",
          $"tracked_editing_session_id"
        )
    ).as("edit_step")
      .join(
        tracked_conflicts,
        $"editing_session_id" === $"tracked_editing_session_id"
      )
      .select(
        tracked_conflicts("*"),
        "interface_user_id"
      )
  }
}
