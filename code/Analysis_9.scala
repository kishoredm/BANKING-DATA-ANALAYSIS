val successCountByEducation = data.filter(col("poutcome") === "success").groupBy("education").count()
println("Count of successful outcomes by education:")
successCountByEducation.limit(5)show()