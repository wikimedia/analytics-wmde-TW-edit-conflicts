import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SaveMode

// TODO: Suite of composable enhancements: LazyTable, ...
object DebugTable extends Defines with SparkSessionWrapper {

  def apply(name: String, table: DataFrame): DataFrame = {
    saveIntermediateTable(table, name)
    table
  }

  def saveIntermediateTable(table: DataFrame, name: String): Unit = {
    table
      .write
      .mode(SaveMode.Overwrite)
      .parquet(dataPathFromName(name))
  }

  def loadIntermediateTable(name: String): DataFrame = {
    spark
      .read
      .parquet(dataPathFromName(name))
      .as(name)
  }
}
