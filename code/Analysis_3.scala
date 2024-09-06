// Calculate the average 'duration' for each 'job' where 'marital' status is 'married'
val avgDurationForMarriedByJob = data.filter(col("marital") === "married")
  .groupBy("job")
  .agg(avg("duration"))
println("Average duration for married individuals by job:")
avgDurationForMarriedByJob.limit(5)show()