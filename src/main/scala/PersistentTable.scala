import org.apache.spark.sql.{AnalysisException, DataFrame, SaveMode}

object PersistentTable extends SparkSessionWrapper
{
  val is_lazy: Boolean = Defines.DEBUG

  def refresh(name: String, calculate: () => DataFrame): DataFrame = {
    if (is_lazy) {
      scala.util.control.Exception.ignoring(classOf[AnalysisException]) {
        return load(name)
      }
    }

    store(name, calculate())
  }

  private def store(name: String, data: DataFrame): DataFrame = {
    data
      .write
      .mode(SaveMode.Overwrite)
      .parquet(Defines.dataPathFromName(name))
    data
      .as(name)
  }

  private def load(name: String): DataFrame = {
    spark
      .read
      .parquet(Defines.dataPathFromName(name))
      .as(name)
  }
}
