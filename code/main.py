from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg
import streamlit as st
import matplotlib.pyplot as plt

def load_data(file_path):
    spark = SparkSession.builder.appName("BankDataAnalysis").getOrCreate()
    df_spark = spark.read.option("header", "true").csv(file_path)
    return df_spark

# Streamlit app
st.set_page_config(page_title="Bank Statistics", page_icon="✈️", layout="wide")

navbar_style = """
    background-image: url('https://images.pexels.com/photos/1098745/pexels-photo-1098745.jpeg?auto=compress&cs=tinysrgb&w=600');
    background-size: cover;
    background-position: center -90px;
    padding: 40px 20px 50px 20px;  /* Adjust the padding to change the height and position */
    border-radius: 10px;
    box-shadow: 0 0 10px rgba(0, 0, 0, 0.1);
"""

st.markdown(
    f"""
    <div style="{navbar_style}" class="streamlit-navbar">
        <h1 style="margin-bottom: 0;">Bank Data Analysis</h1>
    </div>
    """,
    unsafe_allow_html=True,
)

file_path = r"C:\Users\Shyam Surya G\Downloads\banking1.csv"
df = load_data(file_path)

st.sidebar.title("Bank Data Analysis")
selected_analysis = st.sidebar.selectbox("Select Analysis", ["1", "2", "3"])

st.title(f"{selected_analysis} Analysis")

if selected_analysis == "1":
    st.subheader("Single Clients")
    single_clients = df.filter(col("marital") == "single")
    selected_columns = ["age", "job", "education"]
    st.dataframe(single_clients.select(selected_columns).toPandas())

    # Plot the count of single clients by job
    st.subheader("Count of Single Clients by Job")
    job_counts = single_clients.groupBy("job").count().toPandas()
    fig, ax = plt.subplots(figsize=(8, 6))
    ax.bar(job_counts["job"], job_counts["count"])
    ax.set_xlabel("Job")
    ax.set_ylabel("Count")
    st.pyplot(fig)

elif selected_analysis == "2":
    # Calculate the average duration for each job type and marital status
    average_duration_by_job_marital = (
        df.groupBy("job", "marital")
        .agg(avg("duration").alias("average_duration"))
    )

    # Filter for those averages that are above a certain threshold (e.g., 200)
    threshold = 200
    high_average_durations = average_duration_by_job_marital.filter(col("average_duration") > threshold)

    # Display the result
    st.subheader("Job Types and Marital Statuses with High Average Campaign Durations")
    st.dataframe(high_average_durations.toPandas())

    # Plot the result
    fig, ax = plt.subplots(figsize=(10, 6))
    x_values = high_average_durations.select("job", "marital").toPandas().apply(lambda x: f"{x['job']} - {x['marital']}", axis=1).astype(str)
    ax.bar(x_values, high_average_durations["average_duration"])
    ax.set_xlabel("Job - Marital Status")
    ax.set_ylabel("Average Duration")
    ax.set_title("Job Types and Marital Statuses with High Average Campaign Durations")
    plt.xticks(rotation=45, ha="right")
    st.pyplot(fig)

elif selected_analysis == "3":
    # Your code for the third analysis goes here

    # For example, let's calculate the average duration for clients with a housing loan
    avg_duration_for_loan_yes = (
        df.filter(col("loan") == "yes")
        .groupBy("job")
        .agg(avg("duration").alias("average_duration"))
    )

    # Display the result
    st.subheader("Average Duration for Clients with a Housing Loan")
    st.dataframe(avg_duration_for_loan_yes.toPandas())

    # Plot the result
    fig, ax = plt.subplots(figsize=(10, 6))
    ax.bar(avg_duration_for_loan_yes.toPandas()["job"], avg_duration_for_loan_yes.toPandas()["average_duration"])
    ax.set_xlabel("Job")
    ax.set_ylabel("Average Duration")
    ax.set_title("Average Duration for Clients with a Housing Loan")
    plt.xticks(rotation=45, ha="right")
    st.pyplot(fig)

# Add more elif blocks for additional analyses if needed

# Finally, stop the Spark session
spark.stop()
