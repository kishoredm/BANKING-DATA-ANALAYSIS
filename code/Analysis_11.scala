val minAgeForY1 = data.filter(col("y") === 1).agg(min("age"))
println("Minimum age for y=1:")
minAgeForY1.show()