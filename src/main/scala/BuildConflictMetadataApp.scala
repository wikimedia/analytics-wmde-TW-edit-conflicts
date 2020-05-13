/**
 * Take a sample of edit conflict logs, and gather metadata about the conflicting revisions.
 */
object BuildConflictMetadataApp {
  def main(args: Array[String]): Unit = {
    val year = 2020
    val month = 4

    val conflicts = PersistentTable.refresh(
        name = "conflicts",
        calculate = () => QueryConflictPeriod(year, month)
    ).cache()

    val _conflict_rev_details = PersistentTable.refresh(
        name = "conflict_rev_details",
        calculate = () => QueryConflictRevisionMetadata(conflicts, year)
    )

    val exits = PersistentTable.refresh(
        name = "exits",
        calculate = () => QueryConflictExit(conflicts, year)
    )

    val _linked_exits = PersistentTable.refresh(
        name = "linked_exits",
        calculate = () => QueryLinkConflictExit(conflicts, exits)
    )
  }
}
