// Calculate the average 'nr_employed' for each 'marital' status
val avgNrEmployedByMaritalStatus = data.groupBy("marital").agg(avg("nr_employed"))
println("Average nr_employed by marital status:")
avgNrEmployedByMaritalStatus.limit(5)show()