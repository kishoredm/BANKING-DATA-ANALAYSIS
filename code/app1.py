import streamlit as st
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, max, min, sum,desc,corr,col
import matplotlib.pyplot as plt
import seaborn as sns

# Create a Spark session
spark = SparkSession.builder.appName("StreamlitPySparkApp").getOrCreate()

# Load data from a CSV file (replace "bank.csv" with your actual file)
data = spark.read.csv(r"C:\Users\Shyam Surya G\Downloads\banking1.csv", header=True, inferSchema=True)

# Streamlit app
st.title("Streamlit PySpark App")

# Display the original data
st.subheader("Original Data:")
st.dataframe(data.toPandas())

# (1) Single clients
st.subheader("Single Clients:")
single_clients = data.filter("marital = 'single'")
selected_data = single_clients.select("age", "job", "education").toPandas()
st.dataframe(selected_data)

# (2) Visualization: Age distribution of single clients
st.subheader("Age Distribution of Single Clients:(inference:Enthusiasm of age group for setting in life)")
fig, ax = plt.subplots()
sns.histplot(selected_data["age"], bins=20, kde=True, ax=ax)
st.pyplot(fig)

# (3) Additional analysis: Top 10 Job and Marital Combinations with Highest Average Duration
st.subheader("Top 10 Job and Marital Combinations with Highest Average Duration:(inference:Retired people and self employed are more interested in term deposit)")
average_duration_by_job_and_marital = (
    data.groupBy("job", "marital")
    .agg(avg("duration").alias("average_duration"))
)
top_10_duration = (
    average_duration_by_job_and_marital
    .orderBy(col("average_duration").desc())
    .limit(6)
)
fig_top_10 = plt.figure()
sns.barplot(x="job", y="average_duration", hue="marital", data=top_10_duration.toPandas())
plt.title("Top 10 Job and Marital Combinations with Highest Average Duration")
plt.xlabel("Job")
plt.ylabel("Average Duration")
st.pyplot(fig_top_10)

# (4) Additional analysis: Average duration for married individuals by job
st.subheader("Average Duration for Married Individuals by Job:")
avg_duration_for_married_by_job = (
    data.filter(col("marital") == "married")
    .groupBy("job")
    .agg(avg("duration").alias("average_duration"))
    .limit(5)
)
#st.dataframe(avg_duration_for_married_by_job)
fig_avg_duration_married = plt.figure()
sns.barplot(x="job", y="average_duration", data=avg_duration_for_married_by_job.toPandas())
plt.title("Average Duration for Married Individuals by Job:Inference(Retired people need more time to think about term deposit)")
plt.xlabel("Job")
plt.ylabel("Average Duration")
st.pyplot(fig_avg_duration_married)

# (5) Additional analysis: Maximum age for individuals with a loan by education
st.subheader("Minimum Age for Individuals with a Loan by Education:inference(illerate  is getting the loan is getting loan to start a business or improve his economic status)")
max_age_for_loan_by_education = (
    data.filter(col("loan") == "yes")
    .groupBy("education")
    .agg(min("age").alias("min_age"))
    .orderBy(col("min_age").desc())
    .limit(5)
)
fig_max_age_loan_education = plt.figure()
sns.lineplot(x="education", y="min_age", marker="o", data=max_age_for_loan_by_education.toPandas())
plt.title("Minimum Age for Individuals with a Loan by Education")
plt.xlabel("Education")
plt.ylabel("Minimum Age")
st.pyplot(fig_max_age_loan_education)

# (7) Additional analysis: Sum of duration by job (Limit to 5)
st.subheader("Sum of Duration by Job:")
sum_duration_by_job = data.groupBy("job").agg(sum("duration").alias("sum_duration")).limit(5)
fig_sum_duration_by_job = plt.figure()
sns.barplot(x="job", y="sum_duration", data=sum_duration_by_job.toPandas())
plt.title("Sum of Duration by Job:(inference:management people needs info about the term deposit  people are more interested in term deposit)")
plt.xlabel("Job")
plt.ylabel("Sum of Duration")
st.pyplot(fig_sum_duration_by_job)

# (8) Additional analysis: Minimum campaign by marital status (Limit to 5)
st.subheader("Minimum Campaign by Marital Status:")
min_campaign_by_marital_status = data.groupBy("marital").agg(min("campaign").alias("min_campaign")).limit(5)
fig_min_campaign_by_marital = plt.figure()
labels = min_campaign_by_marital_status.toPandas()["marital"]
sizes = min_campaign_by_marital_status.toPandas()["min_campaign"]
plt.pie(sizes, labels=labels, autopct="%1.1f%%", startangle=90)
plt.title("Minimum Campaign by Marital Status")
st.pyplot(fig_min_campaign_by_marital)

# (9) Additional analysis: Maximum previous by education (Limit to 5)
st.subheader("Maximum Previous by Education:inference(people with higher school as the highese education need more information about the term deposits)")
max_previous_by_education = data.groupBy("education").agg(max("previous").alias("max_previous")).limit(5)
fig_max_previous_by_education = plt.figure()
sns.scatterplot(x="education", y="max_previous", data=max_previous_by_education.toPandas())
plt.title("Maximum Previous by Education:inference(people with higher school as the highese education need more information about the term deposits")
plt.xlabel("Education")
plt.ylabel("Maximum Previous")
st.pyplot(fig_max_previous_by_education)

# (10) Additional analysis: Average emp_var_rate by job (Limit to 5)
failure_clients = data.filter(data["poutcome"] == "failure")
# Calculate total duration for each education category
education_duration = failure_clients.groupBy("education").agg(sum("duration").alias("total_duration"))
# Order by total_duration and limit to top 5
top_education = education_duration.orderBy(desc("total_duration")).limit(5)
# Display the result
print("Total duration for top 5 education categories with poutcome = 'failure':Inference(people with university as higest education,even though taking for more duration the previous marketing is not successful )")
top_education.show()

st.subheader("Total Duration by Education Category for Clients with poutcome = 'failure':")
fig_education_duration = plt.figure()
sns.barplot(x="education", y="total_duration", data=top_education.toPandas(), palette="viridis")
plt.title("Total Duration by Education Category (Top 5) for Clients with poutcome = 'failure'")
plt.xlabel("Education Category")
plt.ylabel("Total Duration")
st.pyplot(fig_education_duration)

#Shyam surya-----
st.subheader("Count of Successful Outcomes by Education:")
success_count_by_education = data.filter(col("poutcome") == "success").groupBy("education").count().limit(5)
fig_success_count_by_education = plt.figure()
sns.barplot(x="education", y="count", data=success_count_by_education.toPandas())
plt.title("Count of Successful Outcomes by Education")
plt.xlabel("Education")
plt.ylabel("Count")
st.pyplot(fig_success_count_by_education)

st.subheader("Scatter Plot for Unsuccessful Outcomes with Long Durations:")
unsuccessful_outcomes = data.filter(col("poutcome").isin("nonexistent", "failure"))
long_durations = unsuccessful_outcomes.filter(col("duration") > 300)
fig_scatter_long_durations = plt.figure()
sns.scatterplot(x="duration", y="poutcome", data=long_durations.toPandas())
plt.title("Scatter Plot for Unsuccessful Outcomes with Long Durations")
plt.xlabel("Duration")
plt.ylabel("Poutcome")
st.pyplot(fig_scatter_long_durations)

st.subheader("Pie Chart for Minimum Age for y=1:")
min_age_for_y1 = data.filter(col("y") == 1).agg(min("age")).collect()[0][0]
fig_pie_min_age_for_y1 = plt.figure()
plt.pie([min_age_for_y1, 100 - min_age_for_y1], labels=["Min Age for y=1", "Other"], autopct="%1.1f%%", startangle=90)
plt.title("Minimum Age for y=1")
st.pyplot(fig_pie_min_age_for_y1)

avg_duration_married_y1_by_job = (
    data.filter((col("y") == 1) & (col("marital") == "married"))
    .groupBy("job")
    .agg(avg("duration").alias("avg_duration"))
    .limit(4)
)
#Visualization: Bar plot for Average Duration for married individuals with y=1 by job
st.subheader("Visualization: Average duration for married individuals with y=1 by job")
fig_avg_duration_married_y1 = plt.figure()
sns.barplot(x="job", y="avg_duration", data=avg_duration_married_y1_by_job.toPandas())
plt.title("Average Duration for married individuals with y=1 by job")
plt.xlabel("Job")
plt.ylabel("Average Duration")
st.pyplot(fig_avg_duration_married_y1)

#Shyam surya-------

# (22) Maximum age for individuals with a loan and y=0 by education
max_age_loan_y0_by_education = (
    data.filter((col("y") == 0) & (col("loan") == "yes"))
    .groupBy("education")
    .agg(max("age").alias("max_age"))
    .limit(4)
)

# Visualization: Bar plot for Maximum Age for individuals with a loan and y=0 by education
st.subheader("Visualization: Maximum age for individuals with a loan and y=0 by education")
fig_max_age_loan_y0 = plt.figure()
sns.barplot(x="education", y="max_age", data=max_age_loan_y0_by_education.toPandas())
plt.title("Maximum Age for individuals with a loan and y=0 by education")
plt.xlabel("Education")
plt.ylabel("Maximum Age")
st.pyplot(fig_max_age_loan_y0)

st.subheader("Average nr_employed by Marital Status:")
avg_nr_employed_by_marital_status = (
    data.groupBy("marital")
    .agg(avg("nr_employed").alias("avg_nr_employed"))
    .limit(5)
)
fig_avg_nr_employed_by_marital = plt.figure()
sns.barplot(x="marital", y="avg_nr_employed", data=avg_nr_employed_by_marital_status.toPandas())
plt.title("Average nr_employed by Marital Status")
plt.xlabel("Marital Status")
plt.ylabel("Average nr_employed")
st.pyplot(fig_avg_nr_employed_by_marital)

max_campaign_housing_yes_y0_by_education = (
    data.filter((col("y") == 0) & (col("housing") == "yes"))
    .groupBy("education")
    .agg(max("campaign").alias("max_campaign"))
    .limit(4)
)

# Visualization: Bar plot for Maximum Campaign for individuals with housing and y=0 by education
st.subheader("Visualization: Maximum campaign for individuals with housing and y=0 by education")
fig_max_campaign_housing_yes_y0 = plt.figure()
sns.barplot(x="education", y="max_campaign", data=max_campaign_housing_yes_y0_by_education.toPandas())
plt.title("Maximum Campaign for individuals with housing and y=0 by education")
plt.xlabel("Education")
plt.ylabel("Maximum Campaign")
st.pyplot(fig_max_campaign_housing_yes_y0)

max_campaign_housing_yes_y0_by_education = (
    data.filter((col("y") == 0) & (col("housing") == "yes"))
    .groupBy("education")
    .agg(max("campaign").alias("max_campaign"))
    .limit(4)
)

# Visualization: Bar plot for Maximum Campaign for individuals with housing and y=0 by education
st.subheader("Visualization: Maximum campaign for individuals with housing and y=0 by education")
fig_max_campaign_housing_yes_y0 = plt.figure()
sns.barplot(x="education", y="max_campaign", data=max_campaign_housing_yes_y0_by_education.toPandas())
plt.title("Maximum Campaign for individuals with housing and y=0 by education")
plt.xlabel("Education")
plt.ylabel("Maximum Campaign")
st.pyplot(fig_max_campaign_housing_yes_y0)
