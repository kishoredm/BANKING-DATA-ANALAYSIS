val unsuccessfulOutcomes = data.filter(col("poutcome").isin("nonexistent", "failure"))
val shortDurations = unsuccessfulOutcomes.filter(col("duration") < 300)
shortDurations.limit(5)show()