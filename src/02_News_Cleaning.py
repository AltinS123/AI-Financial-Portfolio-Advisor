#!/usr/bin/env python
# coding: utf-8

# ## 02_News_Cleaning
# 
# null

# In[1]:


from pyspark.sql import functions as F
from pyspark.sql.window import Window

print("=" * 70)
print("FINANCIAL NEWS CLEANING")
print("=" * 70)
print("Reading table: financial_news_raw")


# In[2]:


raw_news_df = spark.table("financial_news_raw")

print("Raw rows:", raw_news_df.count())

display(raw_news_df.limit(10))


# In[3]:


raw_news_df.printSchema()


# In[4]:


clean_news_df = (
    raw_news_df

    # Standardize ticker values
    .withColumn(
        "Ticker",
        F.upper(F.trim(F.col("Ticker")))
    )

    # Clean text columns
    .withColumn(
        "headline",
        F.trim(F.regexp_replace(F.col("headline"), r"\s+", " "))
    )
    .withColumn(
        "summary",
        F.trim(F.regexp_replace(F.col("summary"), r"\s+", " "))
    )
    .withColumn(
        "source",
        F.trim(F.col("source"))
    )

    # Create useful date columns
    .withColumn(
        "Published_Date",
        F.to_date(F.col("datetime"))
    )
    .withColumn(
        "Published_Year",
        F.year(F.col("datetime"))
    )
    .withColumn(
        "Published_Month",
        F.month(F.col("datetime"))
    )
    .withColumn(
        "Published_Hour",
        F.hour(F.col("datetime"))
    )

    # Record when Fabric processed the row
    .withColumn(
        "Processed_At",
        F.current_timestamp()
    )

    # Remove rows without essential data
    .filter(
        F.col("id").isNotNull()
        & F.col("Ticker").isNotNull()
        & F.col("headline").isNotNull()
        & F.col("datetime").isNotNull()
    )

    # Remove duplicates
    .dropDuplicates(["id"])
)


# In[5]:


clean_news_df = clean_news_df.select(
    F.col("id").alias("Article_ID"),
    F.col("Ticker"),
    F.col("datetime").alias("Published_At"),
    F.col("Published_Date"),
    F.col("Published_Year"),
    F.col("Published_Month"),
    F.col("Published_Hour"),
    F.col("source").alias("Source"),
    F.col("headline").alias("Headline"),
    F.col("summary").alias("Summary"),
    F.col("url").alias("Article_URL"),
    F.col("image").alias("Image_URL"),
    F.col("category").alias("Category"),
    F.col("Processed_At")
)

print("Clean rows:", clean_news_df.count())

display(clean_news_df.limit(10))


# In[6]:


(
    clean_news_df
        .write
        .mode("overwrite")
        .format("delta")
        .saveAsTable("financial_news_clean")
)

print("Table financial_news_clean saved successfully.")


# In[7]:


clean_table_df = spark.table("financial_news_clean")

print("Rows stored:", clean_table_df.count())

display(clean_table_df.limit(20))


# In[8]:


display(
    spark.sql("""
        SELECT
            COUNT(*) AS Total_Rows,
            COUNT(DISTINCT Article_ID) AS Unique_Articles,
            COUNT(DISTINCT Ticker) AS Tickers,
            MIN(Published_At) AS Oldest_Article,
            MAX(Published_At) AS Latest_Article
        FROM financial_news_clean
    """)
)


# In[9]:


display(
    spark.sql("""
        SELECT
            SUM(CASE WHEN Article_ID IS NULL THEN 1 ELSE 0 END) AS Missing_ID,
            SUM(CASE WHEN Ticker IS NULL OR TRIM(Ticker) = '' THEN 1 ELSE 0 END) AS Missing_Ticker,
            SUM(CASE WHEN Headline IS NULL OR TRIM(Headline) = '' THEN 1 ELSE 0 END) AS Missing_Headline,
            SUM(CASE WHEN Published_At IS NULL THEN 1 ELSE 0 END) AS Missing_Date
        FROM financial_news_clean
    """)
)


# In[10]:


display(
    spark.sql("""
        SELECT
            Ticker,
            COUNT(*) AS Article_Count,
            ROUND(
                COUNT(*) * 100.0 /
                SUM(COUNT(*)) OVER (),
                2
            ) AS Percentage_Of_All_Articles
        FROM financial_news_clean
        GROUP BY Ticker
        ORDER BY Article_Count DESC
    """)
)


# In[11]:


clean_count = spark.table("financial_news_clean").count()

print("=" * 70)
print("NEWS CLEANING COMPLETED SUCCESSFULLY")
print("=" * 70)
print(f"Rows stored: {clean_count}")
print("Input table: financial_news_raw")
print("Output table: financial_news_clean")
print("Lakehouse: AI_Financial_Lakehouse")

