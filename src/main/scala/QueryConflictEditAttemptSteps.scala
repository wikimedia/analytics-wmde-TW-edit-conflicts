import org.apache.spark.sql.DataFrame

object QueryConflictEditAttemptSteps extends SparkSessionWrapper {
  def apply(conflicts: DataFrame, year: Int): DataFrame = {
    import spark.implicits._

    val trackable_sessions = DebugTable(
      "trackable_sessions",
      spark.sql(
        s"""
           |select
           |  editing_session_id as tracked_editing_session_id
           |from event.editattemptstep
           |where year = ${year}
           |""".stripMargin
      ).as("trackable_session")
        .join(
          conflicts,
          $"wiki" === $"database"
            and $"page_namespace" === $"event.page_ns"
            and $"page_title" === $"event.page_title"
        ).select(
          $"trackable_session.*",
          $"conflict_timestamp" // Terrible but this is our key to quickly re-join later
        )
        .dropDuplicates
    )

    val related_steps = DebugTable(
      "related_steps",
      spark.sql(
        s"""
          |select
          |  to_timestamp(dt) as step_timestamp,
          |  action,
          |  abort_mechanism,
          |  abort_type,
          |  is_oversample,
          |  editing_session_id,
          |  editor_interface,
          |  page_id,
          |  page_ns as page_namespace,
          |  page_title,
          |  user_id,
          |  performer.user_text,
          |  user_editcount,
          |  revision_id as step_rev_id,
          |  database
          |from event.editattemptstep
          |where year = ${year}
          |""".stripMargin
      ).as("edit_step")
      .join(
        trackable_sessions,
        $"editing_session_id" === $"tracked_editing_session_id"
      ).select($"edit_step.*")
      .dropDuplicates
    )
    related_steps
  }
}
