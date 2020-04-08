import org.apache.spark.sql.{DataFrame, SaveMode}

trait Defines {
  val DATA_DIR = "/tmp/awight"

  def dataPathFromName( name: String ): String = {
    s"${DATA_DIR}/${name}"
  }
}
