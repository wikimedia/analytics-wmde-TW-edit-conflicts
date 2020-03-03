import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession


/**
 * Take a sample of edit conflict logs, and gather metadata about the conflicting revisions.
 */
object ConflictApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("Conflict Query").getOrCreate()
    import spark.implicits._

    val conflicts = spark.sql(
      """
        |select
        |  webhost,
        |  wiki,
        |  event.baseRevisionId,
        |  event.latestRevisionId,
        |  event.editCount,
        |  replace(event.textUser, '\n', '\\n') as textbox,
        |  useragent.browser_family,
        |  useragent.os_family
        |from event.twocolconflictconflict
        |where year = 2020 and month = 2
        |""".stripMargin
      )

    val related_revisions = spark
      .sql(
        """
          |select
          |  rev_timestamp,
          |  comment,
          |  performer.user_text,
          |  page_id,
          |  page_namespace,
          |  page_title,
          |  rev_id,
          |  rev_parent_id,
          |  database
          |from event.mediawiki_revision_create
          |where
          |  year = 2020 and month = 2
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
      .dropDuplicates()

    val base_revs = related_revisions
      .select(
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

    val other_revs = related_revisions
      .select(
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

    val next_revs = related_revisions
      .select(
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

    // Recombine datasets into flat output rows.
    val conflict_details = conflicts
      .join(
        base_revs,
        conflicts("baseRevisionId") === $"base_rev_id"
          && conflicts("wiki") === $"base_wiki",
        "left"
      )
      .join(
        other_revs,
        conflicts("latestRevisionId") === $"other_rev_id"
          && conflicts("wiki") === $"other_wiki",
        "left"
      )
      .join(
        next_revs,
        conflicts("latestRevisionId") === $"next_parent_id"
          && conflicts("wiki") === $"next_wiki",
        "left"
      )

    conflict_details
      .write
      .mode(SaveMode.Overwrite)
      .parquet("/tmp/awight/conflicts")

    spark.stop()
  }
}
