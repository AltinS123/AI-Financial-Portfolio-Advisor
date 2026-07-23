# Pipeline Workflow

## Overview

The Microsoft Fabric pipeline automates the complete AI Portfolio Advisor workflow.

---

## Notebook 01 – News Ingestion

Purpose:
Collect financial news from the Finnhub API.

Output:
financial_news_raw

---

## Notebook 02 – News Cleaning

Purpose:
Clean and standardize raw news data.

Tasks:
- Remove duplicates
- Format dates
- Clean text

Output:
financial_news_clean

---

## Notebook 03 – Sentiment Analysis

Purpose:
Analyze financial news using FinBERT.

Tasks:
- Positive classification
- Neutral classification
- Negative classification
- Confidence score

Output:
financial_news_sentiment

---

## Notebook 04 – Market Intelligence

Purpose:
Aggregate article-level sentiment into ticker-level intelligence.

Calculates:

- Average sentiment
- Positive %
- Negative %
- Dominant topic
- Risk level

Output:
ticker_market_intelligence

---

## Notebook 05 – Portfolio Assistant

Purpose:
Combine AI market intelligence with actual portfolio positions.

Reads:

- ticker_market_intelligence
- portfolio_positions
- stock_prices

Produces:

- AI recommendation
- Portfolio-aware analysis
- Risk assessment
- Personalized assistant response

Output:
portfolio_assistant_signals

---

## Semantic Model Refresh

The final pipeline activity refreshes the Microsoft Fabric Semantic Model, ensuring Power BI always displays the latest available data.
