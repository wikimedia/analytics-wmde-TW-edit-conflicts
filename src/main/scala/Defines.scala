import org.apache.spark.sql.{DataFrame, SaveMode}

object Defines {
  /**
   * Can be set to true during development, which enables:
   *   - PersistentTable will try to lazily load the existing stored data, and if present will not recalculate.
   */
  val DEBUG = false

  val DATA_DIR = "/user/awight/edit-conflicts"

  def dataPathFromName(name: String): String = {
    s"${DATA_DIR}/${name}"
  }
}
