val unsuccessfulOutcomes = data.filter(col("poutcome").isin("nonexistent", "failure"))
// Find the instances where 'duration' is above a certain threshold (e.g., 500)
val longDurations = unsuccessfulOutcomes.filter(col("duration") > 500)
longDurations.limit(5)show()