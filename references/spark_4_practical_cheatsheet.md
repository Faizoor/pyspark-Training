# Apache Spark 4.1.x Practical Cheat Sheet

A quick reference for **Spark Core (RDD), Spark SQL/DataFrames, and
Structured Streaming**.\
Focus: **what it does + when to use it + performance guidance**.

------------------------------------------------------------------------

# 1. Spark Core (RDD API)

RDD = **Resilient Distributed Dataset**\
Immutable distributed collection processed in parallel across a cluster.

## RDD Transformations (Lazy)

  -----------------------------------------------------------------------
  Operation         Description       When to Use       Avoid When
  ----------------- ----------------- ----------------- -----------------
  map()             Applies a         When transforming When expanding
                    function to each  records           one record to
                    element.          independently.    many.

  flatMap()         Similar to map    Parsing text or   When only one
                    but outputs       splitting         output per input
                    multiple elements records.          is needed.
                    per input.                          

  filter()          Keeps elements    Remove            Rarely avoided;
                    matching a        unnecessary data  generally safe.
                    condition.        early in          
                                      pipeline.         

  distinct()        Removes duplicate Deduplication     On very large
                    elements.         tasks.            datasets with
                                                        heavy shuffle
                                                        cost.

  union()           Combines two RDDs Merging datasets  When schemas
                    into one.         with same         differ or
                                      structure.        deduplication
                                                        required.

  intersection()    Returns elements  Comparing         Large datasets
                    present in both   datasets.         causing shuffle
                    RDDs.                               overhead.

  subtract()        Removes elements  Filtering         When join logic
                    present in        reference         is more
                    another RDD.      datasets.         appropriate.

  reduceByKey()     Aggregates values Distributed       When all values
                    by key using a    counting or       must be
                    function.         summing tasks.    preserved.

  groupByKey()      Groups values by  When every value  Large datasets
                    key.              must be retained. due to shuffle
                                                        overhead.

  mapValues()       Applies           Processing        When keys must
                    transformation    key‑value         also change.
                    only to values.   datasets.         

  sortByKey()       Sorts key-value   Ordered           Large datasets
                    pairs by key.     processing or     with expensive
                                      reporting.        shuffles.

  join()            Joins two         Enriching         When one dataset
                    key-value RDDs.   datasets.         is very large and
                                                        skewed.

  repartition(n)    Changes           Increase          When decreasing
                    partitions using  parallelism.      partitions.
                    shuffle.                            

  coalesce(n)       Reduces           Before writing    Increasing
                    partitions        output files.     partitions.
                    without full                        
                    shuffle.                            
  -----------------------------------------------------------------------

------------------------------------------------------------------------

## RDD Actions (Trigger Execution)

  -----------------------------------------------------------------------
  Operation               Description             When to Use
  ----------------------- ----------------------- -----------------------
  collect()               Returns all elements to Small datasets only.
                          driver.                 

  count()                 Counts elements in RDD. Dataset size
                                                  estimation.

  first()                 Returns first element.  Sampling dataset
                                                  quickly.

  take(n)                 Returns first n         Inspecting data safely.
                          elements.               

  reduce()                Aggregates dataset      Global aggregation
                          using a function.       tasks.

  foreach()               Executes function on    Writing results to
                          each element.           external systems.

  saveAsTextFile(path)    Writes RDD to           Exporting results.
                          distributed storage.    
  -----------------------------------------------------------------------

------------------------------------------------------------------------

# 2. Spark SQL / DataFrame API

DataFrames represent **structured data with schema** and use the Spark
SQL engine for optimization.

## DataFrame Transformations (Lazy)

  -------------------------------------------------------------------------
  Operation          Description       When to Use       Avoid When
  ------------------ ----------------- ----------------- ------------------
  select()           Chooses specific  Reduce dataset    When all columns
                     columns.          size early.       required.

  withColumn()       Adds or modifies  Feature           Excessive chained
                     a column.         engineering.      transformations.

  drop()             Removes columns.  Cleaning          When columns
                                       unnecessary data. needed later.

  filter()/where()   Filters rows      Apply early for   Rarely avoided.
                     based on          performance.      
                     condition.                          

  groupBy()          Groups rows for   Summaries and     When no
                     aggregation.      analytics.        aggregation
                                                         needed.

  agg()              Performs          Metrics           When row-level
                     aggregations.     calculations.     processing
                                                         required.

  orderBy()/sort()   Sorts dataset.    Reporting         Large distributed
                                       outputs.          datasets
                                                         unnecessarily.

  join()             Combines          Data enrichment.  When datasets are
                     datasets.                           extremely skewed.

  union()            Combines rows of  Appending         Different schemas.
                     two DataFrames.   datasets.         

  distinct()         Removes duplicate Data              Massive datasets
                     rows.             deduplication.    due to shuffle.

  dropDuplicates()   Removes           Data quality      When duplicates
                     duplicates based  cleanup.          are required.
                     on columns.                         

  explode()          Expands           Processing nested Flat datasets.
                     arrays/maps into  data.             
                     rows.                               

  repartition(n)     Redistributes     Increasing        Reducing
                     partitions.       parallelism.      partitions.

  cache()            Stores dataset in Reused datasets   One‑time use
                     memory.           in pipelines.     datasets.

  persist()          Stores dataset    Large pipelines   Memory constrained
                     with configurable needing reuse.    environments.
                     storage level.                      
  -------------------------------------------------------------------------

------------------------------------------------------------------------

## DataFrame Actions

  -----------------------------------------------------------------------
  Operation               Description             When to Use
  ----------------------- ----------------------- -----------------------
  show()                  Displays rows in        Quick debugging.
                          console.                

  collect()               Retrieves all rows to   Small datasets only.
                          driver.                 

  count()                 Returns row count.      Dataset validation.

  first()                 Returns first row.      Quick inspection.

  take(n)                 Retrieves limited rows. Sampling safely.

  describe()              Summary statistics.     Quick exploratory
                                                  analysis.

  write()                 Writes data to storage  Persisting datasets.
                          systems.                

  saveAsTable()           Stores DataFrame as     Data warehouse
                          table.                  pipelines.
  -----------------------------------------------------------------------

------------------------------------------------------------------------

# 3. Structured Streaming

Structured Streaming processes **continuous data streams as incremental
DataFrames**.

------------------------------------------------------------------------

## Streaming Sources

  -----------------------------------------------------------------------
  Source                  Description             When to Use
  ----------------------- ----------------------- -----------------------
  readStream              Creates streaming       Starting streaming
                          DataFrame.              pipeline.

  Kafka source            Reads data from Kafka   Real-time event
                          topics.                 ingestion.

  File source             Monitors directories    Batch-to-stream
                          for new files.          pipelines.

  Socket source           Reads text from TCP     Testing streaming
                          socket.                 pipelines.
  -----------------------------------------------------------------------

------------------------------------------------------------------------

## Streaming Transformations

  -----------------------------------------------------------------------
  Operation               Description             When to Use
  ----------------------- ----------------------- -----------------------
  select()                Selects columns from    Filtering required
                          streaming data.         attributes.

  filter()                Filters incoming        Event validation or
                          events.                 routing.

  groupBy()               Groups records for      Metrics computation.
                          aggregation.            

  agg()                   Aggregates streaming    Streaming analytics.
                          records.                

  join()                  Joins streaming and     Enrichment pipelines.
                          static datasets.        

  window()                Groups events into time Time-based
                          windows.                aggregations.

  withWatermark()         Handles late arriving   Event-time processing
                          data.                   pipelines.
  -----------------------------------------------------------------------

------------------------------------------------------------------------

## Streaming Output Operations

  ------------------------------------------------------------------------
  Operation                Description             When to Use
  ------------------------ ----------------------- -----------------------
  writeStream              Defines streaming sink. Writing streaming
                                                   output.

  outputMode("append")     Writes new rows only.   Event streams without
                                                   updates.

  outputMode("update")     Updates changed rows.   Aggregation pipelines.

  outputMode("complete")   Writes entire result    Small aggregated
                           table.                  outputs.

  trigger()                Defines processing      Controlling latency vs
                           interval.               throughput.

  start()                  Starts streaming query. Beginning execution.

  awaitTermination()       Waits for streaming job Long-running pipelines.
                           completion.             
  ------------------------------------------------------------------------

------------------------------------------------------------------------

# 4. Common Aggregation Functions

  -------------------------------------------------------------------------
  Function                  Description             Use Case
  ------------------------- ----------------------- -----------------------
  count()                   Counts rows or values.  Event counting.

  sum()                     Total of numeric        Revenue or metric
                            column.                 totals.

  avg()                     Average calculation.    KPI analysis.

  min()                     Smallest value.         Monitoring thresholds.

  max()                     Largest value.          Peak detection.

  approx_count_distinct()   Approximate unique      Large-scale analytics.
                            counts.                 
  -------------------------------------------------------------------------

------------------------------------------------------------------------

# 5. Performance Tips

  Tip                      Explanation
  ------------------------ ----------------------------------------------------
  Filter Early             Reduce data size before expensive transformations.
  Avoid groupByKey         Causes large shuffles; prefer reduceByKey.
  Use Broadcast Joins      Efficient when one dataset is small.
  Repartition Carefully    Causes expensive shuffles.
  Cache Only Reused Data   Prevents memory waste.
  Use Built‑in Functions   Faster than Python UDFs.

------------------------------------------------------------------------

# 6. Debugging & Development Tools

  Tool            Purpose
  --------------- -------------------------------------------
  Spark UI        Visualize stages, tasks, and performance.
  explain()       Displays query execution plan.
  printSchema()   Shows DataFrame schema.
  show()          Inspect data quickly.

------------------------------------------------------------------------

# 7. Spark Execution Model

Transformations are **lazy** and build a logical execution plan.

Execution flow:

Transformations\
→ Logical Plan\
→ Catalyst Optimizer\
→ Physical Plan\
→ Tasks on Executors\
→ Actions Trigger Execution

------------------------------------------------------------------------

End of Cheat Sheet
