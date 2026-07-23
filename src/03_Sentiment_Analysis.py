#!/usr/bin/env python
# coding: utf-8

# ## 03_Sentiment_Analysis
# 
# null

# In[1]:


import sys

print("=" * 70)
print("FINANCIAL NEWS SENTIMENT ANALYSIS")
print("=" * 70)
print("Python version:", sys.version)


# In[2]:


try:
    import transformers
    import torch

    print("transformers version:", transformers.__version__)
    print("torch version:", torch.__version__)
    print("AI libraries are available.")

except ImportError as e:
    print("Missing library:", e)


# In[3]:


from pyspark.sql import functions as F
import pandas as pd

clean_news_spark = spark.table("financial_news_clean")

print("Articles available:", clean_news_spark.count())

display(
    clean_news_spark.select(
        "Article_ID",
        "Ticker",
        "Published_At",
        "Headline",
        "Summary"
    ).limit(10)
)


# In[4]:


sentiment_input_df = (
    clean_news_spark
    .withColumn(
        "Sentiment_Text",
        F.concat_ws(
            ". ",
            F.coalesce(F.col("Headline"), F.lit("")),
            F.coalesce(F.col("Summary"), F.lit(""))
        )
    )
    .withColumn(
        "Sentiment_Text",
        F.trim(F.col("Sentiment_Text"))
    )
)

display(
    sentiment_input_df.select(
        "Article_ID",
        "Ticker",
        "Headline",
        "Sentiment_Text"
    ).limit(10)
)


# In[5]:


test_sentiment_pdf = (
    sentiment_input_df
    .select(
        "Article_ID",
        "Ticker",
        "Headline",
        "Sentiment_Text"
    )
    .limit(20)
    .toPandas()
)

print("Test articles:", len(test_sentiment_pdf))

display(test_sentiment_pdf.head())


# In[6]:


from transformers import pipeline

MODEL_NAME = "ProsusAI/finbert"

sentiment_model = pipeline(
    task="text-classification",
    model=MODEL_NAME,
    tokenizer=MODEL_NAME,
    return_all_scores=True
)

print("FinBERT loaded successfully.")


# In[7]:


sample_text = (
    "NVIDIA reported strong revenue growth driven by increasing demand "
    "for artificial intelligence chips."
)

sample_result = sentiment_model(sample_text)

print(sample_result)


# In[8]:


def analyze_sentiment(text):
    """
    Analyze financial text with FinBERT.

    Returns:
        Sentiment
        Confidence
        Sentiment_Score
    """

    if text is None or not str(text).strip():
        return "neutral", 0.0, 0.0

    result = sentiment_model(
        str(text)[:2000],
        truncation=True
    )[0]

    scores = {
        item["label"].lower(): float(item["score"])
        for item in result
    }

    sentiment = max(scores, key=scores.get)
    confidence = scores[sentiment]

    sentiment_score = (
        scores.get("positive", 0.0)
        - scores.get("negative", 0.0)
    )

    return sentiment, confidence, sentiment_score


# In[9]:


test_function_result = analyze_sentiment(
    "NVIDIA reported strong growth and higher demand for AI chips."
)

print(test_function_result)


# In[10]:


sentiment_results = test_sentiment_pdf["Sentiment_Text"].apply(
    analyze_sentiment
)

test_sentiment_pdf[
    [
        "Sentiment",
        "Confidence",
        "Sentiment_Score"
    ]
] = pd.DataFrame(
    sentiment_results.tolist(),
    index=test_sentiment_pdf.index
)


# In[11]:


display(
    test_sentiment_pdf[
        [
            "Ticker",
            "Headline",
            "Sentiment",
            "Confidence",
            "Sentiment_Score"
        ]
    ]
)


# In[12]:


print("Sentiment distribution:")
print(test_sentiment_pdf["Sentiment"].value_counts())

print()
print("Average confidence:")
print(round(test_sentiment_pdf["Confidence"].mean(), 4))

print()
print("Average sentiment score:")
print(round(test_sentiment_pdf["Sentiment_Score"].mean(), 4))


# In[13]:


all_sentiment_pdf = (
    sentiment_input_df
    .select(
        "Article_ID",
        "Sentiment_Text"
    )
    .toPandas()
)

all_sentiment_pdf["Sentiment_Text"] = (
    all_sentiment_pdf["Sentiment_Text"]
    .fillna("")
    .astype(str)
)

print("Articles ready for sentiment analysis:", len(all_sentiment_pdf))


# In[14]:


texts = all_sentiment_pdf["Sentiment_Text"].tolist()

all_predictions = sentiment_model(
    texts,
    batch_size=16,
    truncation=True,
    max_length=512
)

print("Predictions completed:", len(all_predictions))


# In[15]:


def parse_finbert_prediction(prediction):
    scores = {
        item["label"].lower(): float(item["score"])
        for item in prediction
    }

    sentiment = max(scores, key=scores.get)
    confidence = scores[sentiment]

    positive_score = scores.get("positive", 0.0)
    negative_score = scores.get("negative", 0.0)
    neutral_score = scores.get("neutral", 0.0)

    sentiment_score = positive_score - negative_score

    return {
        "Sentiment": sentiment,
        "Confidence": confidence,
        "Positive_Score": positive_score,
        "Negative_Score": negative_score,
        "Neutral_Score": neutral_score,
        "Sentiment_Score": sentiment_score
    }


parsed_predictions = [
    parse_finbert_prediction(prediction)
    for prediction in all_predictions
]

prediction_pdf = pd.DataFrame(parsed_predictions)

display(prediction_pdf.head(10))


# In[16]:


sentiment_results_pdf = pd.concat(
    [
        all_sentiment_pdf[["Article_ID"]].reset_index(drop=True),
        prediction_pdf.reset_index(drop=True)
    ],
    axis=1
)

print("Sentiment result rows:", len(sentiment_results_pdf))

display(sentiment_results_pdf.head(10))


# In[17]:


print("Sentiment distribution:")
print(sentiment_results_pdf["Sentiment"].value_counts())

print()
print("Average confidence:")
print(round(sentiment_results_pdf["Confidence"].mean(), 4))

print()
print("Average sentiment score:")
print(round(sentiment_results_pdf["Sentiment_Score"].mean(), 4))


# In[18]:


sentiment_scores_spark = spark.createDataFrame(
    sentiment_results_pdf
)

print("Spark sentiment rows:", sentiment_scores_spark.count())

display(sentiment_scores_spark.limit(10))


# In[19]:


from pyspark.sql import functions as F

financial_news_sentiment_df = (
    clean_news_spark
    .join(
        sentiment_scores_spark,
        on="Article_ID",
        how="left"
    )
    .withColumn(
        "Bullish_Bearish",
        F.when(F.col("Sentiment") == "positive", F.lit("Bullish"))
         .when(F.col("Sentiment") == "negative", F.lit("Bearish"))
         .otherwise(F.lit("Neutral"))
    )
    .withColumn(
        "Sentiment_Model",
        F.lit("ProsusAI/finbert")
    )
    .withColumn(
        "Scored_At",
        F.current_timestamp()
    )
)

print("Final sentiment rows:", financial_news_sentiment_df.count())

display(
    financial_news_sentiment_df.select(
        "Article_ID",
        "Ticker",
        "Headline",
        "Sentiment",
        "Confidence",
        "Sentiment_Score",
        "Bullish_Bearish"
    ).limit(20)
)


# In[20]:


(
    financial_news_sentiment_df
    .write
    .mode("overwrite")
    .format("delta")
    .saveAsTable("financial_news_sentiment")
)

print("Table financial_news_sentiment saved successfully.")


# In[21]:


display(
    spark.sql("""
        SELECT
            Sentiment,
            Bullish_Bearish,
            COUNT(*) AS Article_Count,
            ROUND(AVG(Confidence), 4) AS Average_Confidence,
            ROUND(AVG(Sentiment_Score), 4) AS Average_Sentiment_Score
        FROM financial_news_sentiment
        GROUP BY Sentiment, Bullish_Bearish
        ORDER BY Article_Count DESC
    """)
)


# In[22]:


display(
    spark.sql("""
        SELECT
            Ticker,
            COUNT(*) AS Articles_Analyzed,
            SUM(CASE WHEN Sentiment = 'positive' THEN 1 ELSE 0 END)
                AS Positive_Articles,
            SUM(CASE WHEN Sentiment = 'neutral' THEN 1 ELSE 0 END)
                AS Neutral_Articles,
            SUM(CASE WHEN Sentiment = 'negative' THEN 1 ELSE 0 END)
                AS Negative_Articles,
            ROUND(AVG(Sentiment_Score), 4)
                AS Average_Sentiment_Score,
            ROUND(AVG(Confidence), 4)
                AS Average_Confidence
        FROM financial_news_sentiment
        GROUP BY Ticker
        ORDER BY Average_Sentiment_Score DESC
    """)
)

