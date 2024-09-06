// Calculate the average duration for each job type and marital status
val avgDurationForMarriedAndY1ByJob = data.filter(col("y") === 1)
  .filter(col("marital") === "married")
  .groupBy("job")
  .agg(avg("duration")
    .alias("avg_duration"))
// Order by avg_duration in descending order and limit to top 6 jobs
val orderedAvgDurationForMarriedAndY1ByJob = avgDurationForMarriedAndY1ByJob.orderBy(desc("avg_duration"))
println("Top 6 jobs for married individuals with y=1 by average duration:")
orderedAvgDurationForMarriedAndY1ByJob.limit(6)show()