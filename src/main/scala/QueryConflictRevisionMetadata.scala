import org.apache.spark.sql.DataFrame

object QueryConflictRevisionMetadata extends SparkSessionWrapper {
  def apply(conflicts: DataFrame, year: Int): DataFrame = {
    import spark.implicits._

    val related_revisions = DebugTable(
      "related_revisions",
      spark.sql(
        s"""
          |select
          |  to_timestamp(rev_timestamp) as rev_timestamp,
          |  performer.user_id,
          |  performer.user_text,
          |  performer.user_edit_count,
          |  page_id,
          |  page_namespace,
          |  page_title,
          |  rev_id,
          |  rev_parent_id,
          |  database
          |from event.mediawiki_revision_create
          |where year = ${year}
          |""".stripMargin
      ).as("revision_create")
      .join(
        conflicts,
        $"wiki" === $"database"
          and (
          $"baseRevisionId" === $"rev_id"
            or $"latestRevisionId" === $"rev_id"
            or $"latestRevisionId" === $"rev_parent_id"
          )
      ).select($"revision_create.*")
      .dropDuplicates
    )

    val base_revs = DebugTable(
      "base_revs",
      related_revisions.select(
        $"rev_timestamp".as("base_timestamp"),
        $"comment".as("base_comment"),
        $"user_text".as("base_user"),
        $"page_id",
        $"page_namespace",
        $"page_title",
        $"rev_id".as("base_rev_id"),
        $"database".as("base_wiki")
      ).as("base_revs")
      .join(
        conflicts,
        $"baseRevisionId" === $"base_rev_id"
          && $"wiki" === $"base_wiki"
      ).select($"base_revs.*")
      .dropDuplicates
    )

    val other_revs = DebugTable(
      "other_revs",
      related_revisions.select(
        $"rev_timestamp".as("other_timestamp"),
        $"comment".as("other_comment"),
        $"user_text".as("other_user"),
        $"rev_id".as("other_rev_id"),
        $"database".as("other_wiki")
      ).as("other_revs")
      .join(
        conflicts,
        $"latestRevisionId" === $"other_rev_id"
          && $"wiki" === $"other_wiki"
      ).select($"other_revs.*")
      .dropDuplicates
    )

    val next_revs = DebugTable(
      "next_revs",
      related_revisions.select(
        $"rev_timestamp".as("next_timestamp"),
        $"comment".as("next_comment"),
        $"user_text".as("next_user"),
        $"rev_id".as("next_rev_id"),
        $"rev_parent_id".as("next_parent_id"),
        $"database".as("next_wiki")
      ).as("next_revs")
      .join(
        conflicts,
        $"latestRevisionId" === $"next_parent_id"
          && $"wiki" === $"next_wiki"
      ).select($"next_revs.*")
      .dropDuplicates
    )

    // Recombine datasets into flat output rows.
    conflicts.join(
      base_revs,
      conflicts("baseRevisionId") === $"base_rev_id"
        && conflicts("wiki") === $"base_wiki",
      "left"
    ).join(
    other_revs,
    conflicts("latestRevisionId") === $"other_rev_id"
      && conflicts("wiki") === $"other_wiki",
      "left"
    ).join(
      next_revs,
      conflicts("latestRevisionId") === $"next_parent_id"
        && conflicts("wiki") === $"next_wiki",
      "left"
    )
  }
}
