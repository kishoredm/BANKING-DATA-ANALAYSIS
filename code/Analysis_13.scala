val maxAgeForLoanAndY0ByEducation = data.filter(col("y") === 0)
  .filter(col("loan") === "yes")
  .groupBy("education")
  .agg(max("age"))
println("Maximum age for individuals with a loan and y=0 by education:")
maxAgeForLoanAndY0ByEducation.limit(4).show()