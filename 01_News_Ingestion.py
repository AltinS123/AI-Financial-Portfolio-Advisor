#!/usr/bin/env python
# coding: utf-8

# ## 01_News_Ingestion
# 
# null

# In[1]:


from datetime import datetime
import pandas as pd
import requests

print("=" * 60)
print("AI Financial Market Intelligence Project")
print("=" * 60)
print(f"Project Started: {datetime.now()}")
print("Lakehouse Connected Successfully")


# In[2]:


portfolio_tickers = [
    "NVDA",
    "META",
    "AMD",
    "AAPL",
    "AMZN",
    "GOOGL",
    "IBM",
    "SOFI",
    "TSLA",
    "PLTR",
    "RGTI",
    "NFLX",
    "MCD",
    "NKE",
    "SHEL",
    "NOK"
]

print(f"Monitoring {len(portfolio_tickers)} stocks:")
print(portfolio_tickers)


# In[4]:


from datetime import datetime, timedelta, timezone
import requests
import pandas as pd
import time
import os

FINNHUB_API_KEY = os.getenv("FINNHUB_API_KEY")

if not FINNHUB_API_KEY:
    raise ValueError(
        "Please configure the FINNHUB_API_KEY environment variable before running this script."
    )

# We begin with 7 days so we have enough articles for testing.
date_to = datetime.now(timezone.utc).date()
date_from = date_to - timedelta(days=7)

print("News period:", date_from, "to", date_to)
print("Tickers:", len(portfolio_tickers))


# In[5]:


test_ticker = "NVDA"

url = "https://finnhub.io/api/v1/company-news"

params = {
    "symbol": test_ticker,
    "from": str(date_from),
    "to": str(date_to),
    "token": FINNHUB_API_KEY
}

response = requests.get(
    url,
    params=params,
    timeout=30
)

print("HTTP status:", response.status_code)

response.raise_for_status()

test_news = response.json()

print("Articles returned:", len(test_news))


# In[ ]:





# In[6]:


if len(test_news) > 0:
    for article in test_news[:5]:
        print("=" * 90)
        print("Headline:", article.get("headline"))
        print("Source:", article.get("source"))
        print("URL:", article.get("url"))
        print("Published timestamp:", article.get("datetime"))
else:
    print("No NVIDIA articles were returned for the selected period.")


# In[7]:


nvda_test_df = pd.DataFrame(test_news)

print("Rows:", len(nvda_test_df))
print("Columns:", list(nvda_test_df.columns))

display(nvda_test_df.head(10))


# In[8]:


nvda_test_df["Published_Date"] = pd.to_datetime(
    nvda_test_df["datetime"],
    unit="s"
)

display(
    nvda_test_df[
        [
            "Published_Date",
            "related",
            "headline",
            "source",
            "summary"
        ]
    ].head(10)
)


# In[9]:


print(nvda_test_df.info())


# In[10]:


print("Total Articles:", len(nvda_test_df))
print("Unique IDs:", nvda_test_df["id"].nunique())


# In[13]:


def get_company_news(ticker, date_from, date_to, api_key):
    """
    Download company news for one ticker from Finnhub.
    """

    url = "https://finnhub.io/api/v1/company-news"

    params = {
        "symbol": ticker,
        "from": str(date_from),
        "to": str(date_to),
        "token": api_key
    }

    response = requests.get(url, params=params, timeout=30)

    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error downloading {ticker}: {response.status_code}")
        return []


# In[14]:


all_news = []

for ticker in portfolio_tickers:

    print(f"Downloading {ticker}...")

    news = get_company_news(
        ticker,
        date_from,
        date_to,
        FINNHUB_API_KEY
    )

    for article in news:

        article["Ticker"] = ticker

        all_news.append(article)

print()
print("="*60)
print("Download completed!")
print("Total Articles:", len(all_news))


# In[15]:


news_df = pd.DataFrame(all_news)

print(news_df.shape)

display(news_df.head())


# In[16]:


news_df["datetime"] = pd.to_datetime(
    news_df["datetime"],
    unit="s",
    utc=True
)

news_df = news_df.rename(columns={
    "related": "Related"
})

news_df = news_df[
    [
        "datetime",
        "Ticker",
        "headline",
        "summary",
        "source",
        "url",
        "image",
        "category",
        "id"
    ]
]

display(news_df.head())


# In[17]:


before = len(news_df)

news_df = news_df.drop_duplicates(subset=["id"])

after = len(news_df)

print("Before:", before)
print("After :", after)
print("Removed:", before - after)


# In[18]:


print(news_df.info())

display(news_df.sample(10))


# In[19]:


spark_news_df = spark.createDataFrame(news_df)

print("Rows:", spark_news_df.count())

display(spark_news_df.limit(10))


# In[20]:


(
    spark_news_df
        .write
        .mode("overwrite")
        .format("delta")
        .saveAsTable("financial_news_raw")
)


# In[ ]:


display(
    spark.table("financial_news_raw")
)


# In[23]:


raw_count = spark.table("financial_news_raw").count()

print("Rows saved in financial_news_raw:", raw_count)


# In[24]:


display(
    spark.sql("""
        SELECT
            Ticker,
            COUNT(*) AS Article_Count,
            MIN(datetime) AS Oldest_Article,
            MAX(datetime) AS Latest_Article
        FROM financial_news_raw
        GROUP BY Ticker
        ORDER BY Article_Count DESC
    """)
)


# In[25]:


ticker_count = spark.sql("""
    SELECT COUNT(DISTINCT Ticker) AS Ticker_Count
    FROM financial_news_raw
""").first()["Ticker_Count"]

print("Distinct tickers stored:", ticker_count)


# In[26]:


display(
    spark.sql("""
        SELECT
            COUNT(*) AS Total_Rows,
            COUNT(DISTINCT id) AS Unique_Article_IDs,
            COUNT(*) - COUNT(DISTINCT id) AS Duplicate_Rows
        FROM financial_news_raw
    """)
)


# In[27]:


print("=" * 70)
print("NEWS INGESTION COMPLETED SUCCESSFULLY")
print("=" * 70)
print(f"Rows stored: {raw_count}")
print(f"Tickers stored: {ticker_count}")
print("Output table: financial_news_raw")
print("Lakehouse: AI_Financial_Lakehouse")

