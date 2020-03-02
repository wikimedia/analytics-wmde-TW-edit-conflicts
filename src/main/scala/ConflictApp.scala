import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession


object ConflictApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("Conflict Query").getOrCreate()

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
        |""".stripMargin
      )

    val base_revs = spark.sql(
      """
        |select
        |  event_timestamp as base_timestamp,
        |  event_comment as base_comment,
        |  event_user_text as base_user,
        |  page_id,
        |  page_namespace,
        |  page_title,
        |  revision_id as base_rev_id,
        |  wiki_db
        |from wmf.mediawiki_history
        |where
        |  event_entity = 'revision'
        |  and event_type = 'create'
        |""".stripMargin
    )

    val other_revs = spark.sql(
      """
        |select
        |  event_timestamp as other_timestamp,
        |  event_comment as other_comment,
        |  event_user_text as other_user,
        |  revision_id as other_rev_id,
        |  wiki_db
        |from wmf.mediawiki_history
        |where
        |  event_entity = 'revision'
        |  and event_type = 'create'
        |""".stripMargin
    )

    val next_revs = spark.sql(
      """
        |select
        |  event_timestamp as next_timestamp,
        |  event_comment as next_comment,
        |  event_user_text as next_user,
        |  revision_id as next_rev_id,
        |  revision_parent_id as next_parent_id,
        |  wiki_db
        |from wmf.mediawiki_history
        |where
        |  event_entity = 'revision'
        |  and event_type = 'create'
        |""".stripMargin
    )

    val conflict_details = conflicts
      .join(
        base_revs,
        conflicts("baseRevisionId") === base_revs("base_rev_id")
          and conflicts("wiki") === base_revs("wiki_db"),
        "left"
      )
      .join(
        other_revs,
        conflicts("latestRevisionId") === other_revs("other_rev_id")
          and conflicts("wiki") === other_revs("wiki_db"),
        "left"
      )
      .join(
        next_revs,
        conflicts("latestRevisionId") === next_revs("next_parent_id")
          and conflicts("wiki") === next_revs("wiki_db"),
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
