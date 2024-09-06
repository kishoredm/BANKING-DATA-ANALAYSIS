// Find the maximum 'age' for each 'education' level where 'loan' is 'yes'
val minAgeForLoanByEducation = data.filter(col("loan") === "yes").groupBy("education").agg(min("age"))
println("Minimum age for individuals with a loan by education:")
minAgeForLoanByEducation.show()