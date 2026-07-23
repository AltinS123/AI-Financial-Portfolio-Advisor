#!/usr/bin/env python
# coding: utf-8

# ## 04_AI_Topic_Extraction
# 
# null

# In[1]:


import sys
import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from transformers import pipeline

print("=" * 70)
print("AI FINANCIAL NEWS TOPIC EXTRACTION")
print("=" * 70)
print("Python version:", sys.version)

# ---------------------------------------------------------
# 1. Load sentiment table
# ---------------------------------------------------------

sentiment_news_spark = spark.table("financial_news_sentiment")

print("Articles available:", sentiment_news_spark.count())

# ---------------------------------------------------------
# 2. Define topic labels
# ---------------------------------------------------------

financial_topics = [
    "earnings and revenue",
    "artificial intelligence and technology",
    "analyst ratings and price targets",
    "product launch and innovation",
    "mergers and acquisitions",
    "regulation and government policy",
    "legal issues and lawsuits",
    "competition and market share",
    "valuation and stock price",
    "management and leadership",
    "partnerships and contracts",
    "economic and market conditions",
    "dividends and shareholder returns",
    "cost reduction and restructuring",
    "cybersecurity and data privacy"
]

print("Number of financial topics:", len(financial_topics))

# ---------------------------------------------------------
# 3. Load topic model once
# ---------------------------------------------------------

TOPIC_MODEL_NAME = "facebook/bart-large-mnli"

topic_model = pipeline(
    task="zero-shot-classification",
    model=TOPIC_MODEL_NAME,
    tokenizer=TOPIC_MODEL_NAME,
    device=-1
)

print("Topic classification model loaded successfully.")

# ---------------------------------------------------------
# 4. Build ticker-level metrics
# ---------------------------------------------------------

ticker_metrics_spark = (
    sentiment_news_spark
    .groupBy("Ticker")
    .agg(
        F.count("*").alias("Articles_Analyzed"),

        F.sum(
            F.when(F.col("Sentiment") == "positive", 1).otherwise(0)
        ).alias("Positive_Articles"),

        F.sum(
            F.when(F.col("Sentiment") == "neutral", 1).otherwise(0)
        ).alias("Neutral_Articles"),

        F.sum(
            F.when(F.col("Sentiment") == "negative", 1).otherwise(0)
        ).alias("Negative_Articles"),

        F.avg("Sentiment_Score").alias("Average_Sentiment_Score"),

        F.avg("Confidence").alias("Average_Sentiment_Confidence"),

        F.max("Published_At").alias("Latest_Article_At")
    )
)

# ---------------------------------------------------------
# 5. Take the latest 20 headlines per ticker
# ---------------------------------------------------------

headline_window = (
    Window
    .partitionBy("Ticker")
    .orderBy(F.desc("Published_At"))
)

recent_headlines_spark = (
    sentiment_news_spark
    .select(
        "Ticker",
        "Published_At",
        "Headline"
    )
    .filter(F.col("Headline").isNotNull())
    .withColumn(
        "Headline_Rank",
        F.row_number().over(headline_window)
    )
    .filter(F.col("Headline_Rank") <= 20)
)

ticker_text_spark = (
    recent_headlines_spark
    .groupBy("Ticker")
    .agg(
        F.concat_ws(
            " | ",
            F.collect_list("Headline")
        ).alias("Ticker_News_Text")
    )
)

ticker_topic_pdf = (
    ticker_text_spark
    .orderBy("Ticker")
    .toPandas()
)

print("Tickers prepared:", len(ticker_topic_pdf))

# ---------------------------------------------------------
# 6. Extract topics once
# ---------------------------------------------------------

def extract_ticker_topics(text):

    if text is None or not str(text).strip():
        return (
            "unclassified",
            0.0,
            "unclassified",
            0.0
        )

    result = topic_model(
        str(text)[:3000],
        candidate_labels=financial_topics,
        multi_label=True,
        hypothesis_template=(
            "The financial news for this company is about {}."
        ),
        truncation=True
    )

    return (
        result["labels"][0],
        float(result["scores"][0]),
        result["labels"][1],
        float(result["scores"][1])
    )


ticker_topic_results = (
    ticker_topic_pdf["Ticker_News_Text"]
    .apply(extract_ticker_topics)
)

ticker_topic_pdf[
    [
        "Primary_Topic",
        "Primary_Topic_Confidence",
        "Secondary_Topic",
        "Secondary_Topic_Confidence"
    ]
] = pd.DataFrame(
    ticker_topic_results.tolist(),
    index=ticker_topic_pdf.index
)

# ---------------------------------------------------------
# 7. Convert topics to Spark
# ---------------------------------------------------------

ticker_topics_spark = spark.createDataFrame(
    ticker_topic_pdf[
        [
            "Ticker",
            "Primary_Topic",
            "Primary_Topic_Confidence",
            "Secondary_Topic",
            "Secondary_Topic_Confidence"
        ]
    ]
)

# ---------------------------------------------------------
# 8. Create final market intelligence table
# ---------------------------------------------------------

ticker_intelligence_df = (
    ticker_metrics_spark
    .join(
        ticker_topics_spark,
        on="Ticker",
        how="left"
    )
    .withColumn(
        "Positive_Percentage",
        F.round(
            F.col("Positive_Articles")
            / F.col("Articles_Analyzed") * 100,
            2
        )
    )
    .withColumn(
        "Negative_Percentage",
        F.round(
            F.col("Negative_Articles")
            / F.col("Articles_Analyzed") * 100,
            2
        )
    )
    .withColumn(
        "AI_Signal",
        F.when(
            (F.col("Average_Sentiment_Score") >= 0.20)
            & (F.col("Positive_Percentage") >= 45),
            "Positive"
        )
        .when(
            F.col("Average_Sentiment_Score") <= -0.20,
            "Caution"
        )
        .otherwise("Hold")
    )
    .withColumn(
        "Risk_Level",
        F.when(
            F.col("Negative_Percentage") >= 45,
            "High"
        )
        .when(
            F.col("Negative_Percentage") >= 30,
            "Medium"
        )
        .otherwise("Low")
    )
    .withColumn(
        "Signal_Reason",
        F.concat(
            F.lit("Average sentiment score: "),
            F.round(
                F.col("Average_Sentiment_Score"),
                3
            ).cast("string"),
            F.lit("; Positive news: "),
            F.col("Positive_Percentage").cast("string"),
            F.lit("%; Main topic: "),
            F.coalesce(
                F.col("Primary_Topic"),
                F.lit("unclassified")
            )
        )
    )
    .withColumn(
        "Generated_At",
        F.current_timestamp()
    )
)

# ---------------------------------------------------------
# 9. Save once
# ---------------------------------------------------------

(
    ticker_intelligence_df
    .write
    .mode("overwrite")
    .format("delta")
    .saveAsTable("ticker_market_intelligence")
)

print("Table ticker_market_intelligence saved successfully.")

# ---------------------------------------------------------
# 10. Validation
# ---------------------------------------------------------

display(
    ticker_intelligence_df.select(
        "Ticker",
        "Articles_Analyzed",
        "Positive_Percentage",
        "Negative_Percentage",
        "Average_Sentiment_Score",
        "Primary_Topic",
        "Secondary_Topic",
        "Risk_Level",
        "AI_Signal",
        "Signal_Reason"
    ).orderBy(
        F.desc("Average_Sentiment_Score")
    )
)

print("=" * 70)
print("MARKET INTELLIGENCE COMPLETED SUCCESSFULLY")
print("=" * 70)

