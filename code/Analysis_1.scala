//single clients
val singleClients = data.filter("marital = 'single'")
// You can further select specific columns if needed
singleClients.select("age", "job", "education").show()