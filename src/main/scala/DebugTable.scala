import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SaveMode

object DebugTable extends Defines {
  def apply(name: String, table: DataFrame): DataFrame = {
    saveIntermediateTable(table, name)
    table
  }

  def saveIntermediateTable(table: DataFrame, name: String): Unit = {
    table
      .write
      .mode(SaveMode.Overwrite)
      .parquet(s"${DATA_DIR}/${name}")
  }
}
