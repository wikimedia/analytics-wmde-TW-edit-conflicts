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
    val revisions = spark.sql(
      """
        |select
        |  event_timestamp,
        |  event_comment,
        |  event_user_text,
        |  page_id,
        |  page_namespace,
        |  page_title,
        |  revision_id,
        |  wiki_db
        |from wmf.mediawiki_history
        |where
        |  event_entity = 'revision'
        |  and event_type = 'create'
        |""".stripMargin
      )

    val conflict_details = conflicts.join(
      revisions,
      conflicts("baseRevisionId") === revisions("revision_id")
        and conflicts("wiki") === revisions("wiki_db"))

    conflict_details
      .coalesce(1)
      .write
      .option("header", "true")
      .mode(SaveMode.Overwrite)
      .csv("/tmp/awight/conflicts")
    spark.stop()
  }
}
