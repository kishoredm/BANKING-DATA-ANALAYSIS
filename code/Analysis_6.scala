// Calculate the minimum 'campaign' for each 'marital' status
val minCampaignByMaritalStatus = data.groupBy("marital").agg(min("campaign"))
println("Minimum campaign by marital status:")
minCampaignByMaritalStatus.show()