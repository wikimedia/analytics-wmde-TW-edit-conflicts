import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession


/**
 * Take a sample of edit conflict logs, and gather metadata about the conflicting revisions.
 *
 * TODO:
 *   - Is there a way to do a single scan of revisions?  Are we doing three here?
 *   - Don't need all the left-hand columns when joining, maybe these can be a pure "where" instead.
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
        |  replace(event.textUser, '\n', '\\n'),
        |  useragent.browser_family,
        |  useragent.os_family
        |from event.twocolconflictconflict
        |where year = 2020 and month = 2
        |-- FIXME: pilot run
        |limit 2
        |""".stripMargin
      )

    val base_revs = spark
      .sql(
        """
          |select
          |  event_timestamp as base_timestamp,
          |  event_comment as base_comment,
          |  event_user_text as base_user,
          |  page_id,
          |  page_namespace,
          |  page_title,
          |  revision_id as base_rev_id,
          |  wiki_db as base_wiki
          |from wmf.mediawiki_history
          |where
          |  event_entity = 'revision'
          |  and event_type = 'create'
          |""".stripMargin
      ).as("rev_create")
      .join(
        conflicts,
        $"baseRevisionId" === $"base_rev_id"
          && $"wiki" === $"base_wiki"
      ).select($"rev_create.*")

    val other_revs = spark
      .sql(
        """
          |select
          |  event_timestamp as other_timestamp,
          |  event_comment as other_comment,
          |  event_user_text as other_user,
          |  revision_id as other_rev_id,
          |  wiki_db as other_wiki
          |from wmf.mediawiki_history
          |where
          |  event_entity = 'revision'
          |  and event_type = 'create'
          |""".stripMargin
      ).as("rev_create")
      .join(
        conflicts,
        $"latestRevisionId" === $"other_rev_id"
          && $"wiki" === $"other_wiki"
      ).select($"rev_create.*")

    val next_revs = spark
      .sql(
        """
          |select
          |  event_timestamp as next_timestamp,
          |  event_comment as next_comment,
          |  event_user_text as next_user,
          |  revision_id as next_rev_id,
          |  revision_parent_id as next_parent_id,
          |  wiki_db as next_wiki
          |from wmf.mediawiki_history
          |where
          |  event_entity = 'revision'
          |  and event_type = 'create'
          |""".stripMargin
      ).as("rev_create")
      .join(
        conflicts,
        $"latestRevisionId" === $"next_parent_id"
          && $"wiki" === $"next_wiki"
      ).select($"rev_create.*")

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
      .coalesce(1)
      .write
      .option("header", "true")
      .mode(SaveMode.Overwrite)
      .csv("/tmp/awight/conflicts")

    spark.stop()
  }
}
