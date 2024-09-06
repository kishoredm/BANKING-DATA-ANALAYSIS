// Calculate the maximum 'previous' for each 'education' level
val maxPreviousByEducation = data.groupBy("education").agg(max("previous"))
println("Maximum previous by education:")
maxPreviousByEducation.limit(5)show()