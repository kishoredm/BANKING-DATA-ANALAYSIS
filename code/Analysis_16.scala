val minCampaignForNoHousingByJob = data.filter(col("housing") === "no").groupBy("job").agg(min("campaign"))
println("Minimum campaign for individuals without housing by job:")
minCampaignForNoHousingByJob.limit(5)show()

spark.stop()