Analytics reporting for MediaWiki edit conflicts and the
[TwoColConflict](https://www.mediawiki.org/wiki/Extension:TwoColConflict)
extension.

* `reports`: [SWAP](https://wikitech.wikimedia.org/wiki/SWAP) notebooks to
explore the data.
* `src`: Scala Spark code to refine EventLogging traces into a more usable
form.

## Running
Build the jar:

    sbt package

Process data:

    spark2-submit \
        --class BuildConflictMetadataApp \
        --master yarn \
        --executor-memory 8G --executor-cores 4 --driver-memory 16G \
        --conf spark.dynamicAllocation.maxExecutors=64 \
        ./target/scala-2.11/conflict-spark-2_2.11-0.1.jar
