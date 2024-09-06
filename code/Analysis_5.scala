// Calculate the sum of 'duration' for each 'job'
val sumDurationByJob = data.groupBy("job").agg(sum("duration"))
println("Sum of duration by job:")
sumDurationByJob.show()