# 🤖 AI Portfolio Advisor using Microsoft Fabric

An end-to-end AI-powered investment decision support system built using **Microsoft Fabric**, **Python**, **Power BI**, and **Natural Language Processing (NLP)**.

The solution combines live financial news, AI sentiment analysis, market intelligence, and portfolio holdings to generate personalized investment insights and recommendations.

---

# Project Overview

Traditional investment dashboards focus mainly on historical prices and portfolio performance.

This project extends portfolio analytics by integrating Artificial Intelligence into the investment workflow.

The system automatically:

- Collects financial news from online sources
- Cleans and processes articles
- Performs sentiment analysis using FinBERT
- Identifies dominant market topics
- Calculates AI confidence scores
- Combines market intelligence with portfolio positions
- Generates personalized investment recommendations
- Presents everything inside an interactive Power BI dashboard

---

# Technologies Used

- Microsoft Fabric
- Microsoft Fabric Lakehouse
- OneLake
- Microsoft Fabric Pipelines
- Delta Lake
- Power BI
- Python
- PySpark
- Pandas
- Hugging Face Transformers
- FinBERT
- Finnhub API

---

# Solution Architecture

```
Financial News API
        │
        ▼
Notebook 01
News Ingestion
        │
        ▼
Notebook 02
News Cleaning
        │
        ▼
Notebook 03
Sentiment Analysis (FinBERT)
        │
        ▼
Notebook 04
Market Intelligence
        │
        ▼
Notebook 05
Portfolio Assistant
        │
        ▼
Delta Tables
        │
        ▼
Semantic Model
        │
        ▼
Power BI Dashboard
```

---

# Microsoft Fabric Components

## Lakehouse

AI_Financial_Lakehouse

---

## Pipeline

AI_Financial_Intelligence_Pipeline

Pipeline stages:

- News Ingestion
- News Cleaning
- Sentiment Analysis
- Market Intelligence
- Portfolio Assistant
- Semantic Model Refresh

---

## Semantic Model

AI_Financial_Intelligence_Model

---

# Data Pipeline

The pipeline automatically performs:

### Step 1

Download financial news from external APIs.

### Step 2

Clean and normalize articles.

### Step 3

Run FinBERT sentiment analysis.

Outputs:

- Positive
- Neutral
- Negative

### Step 4

Extract dominant financial topics using NLP classification.

Examples:

- Artificial Intelligence
- Earnings
- Valuation
- Market Conditions
- Product Launches

### Step 5

Aggregate ticker-level intelligence.

Calculate:

- Positive %
- Negative %
- Confidence
- Risk
- Market Signal

### Step 6

Combine market intelligence with portfolio holdings.

Generate personalized recommendations.

---

# Dashboard Features

## Portfolio Summary

- Current Shares
- Average Cost
- Current Price
- Market Value
- Unrealized Profit/Loss
- Unrealized Return
- AI Confidence

---

## AI Analysis

Displays:

- Recommendation
- Risk Level
- Overall Sentiment
- AI Confidence

---

## AI Portfolio Advisor

Generates a personalized explanation for each investment.

Example:

> You currently own IBM shares.

> Recent financial news shows predominantly negative sentiment.

> Your position is currently at an unrealized loss.

> Current recommendation is to hold rather than average down while negative sentiment persists.

---

## News Insights

Displays:

- Positive Articles %
- Negative Articles %
- Dominant News Topic

---

# Data Model

Tables:

- financial_news_raw
- financial_news_clean
- financial_news_sentiment
- ticker_market_intelligence
- portfolio_assistant_signals
- portfolio_positions
- stock_prices

---

# AI Models

### Sentiment Analysis

FinBERT

Outputs:

- Positive
- Neutral
- Negative

---

### Topic Classification

Zero-shot classification

Topics include:

- Earnings
- Artificial Intelligence
- Regulation
- Valuation
- Partnerships
- Market Conditions

---

# Power BI Dashboard

The report provides:

- Interactive ticker selection
- AI-generated investment recommendations
- Portfolio metrics
- News insights
- Risk analysis
- Confidence scoring

---

# Skills Demonstrated

Microsoft Fabric

Power BI

Python

PySpark

Delta Lake

OneLake

Microsoft Fabric Pipelines

Semantic Models

Natural Language Processing

Artificial Intelligence

Financial Data Analytics

ETL Development

Data Engineering

Dashboard Design

Business Intelligence

---

# Repository Structure

```
Images/
Notebooks/
Python/
Power BI/
Documentation/
Sample Data/
```

---

# Future Improvements

- Integration with additional financial news providers
- Real-time streaming using Eventstreams
- Large Language Model (LLM) explanations
- Price forecasting using Machine Learning
- Email alerts for significant sentiment changes
- Portfolio optimization suggestions
- Mobile-friendly Power BI report

---

# Dashboard Preview

https://github.com/AltinS123/AI-Financial-Portfolio-Advisor/blob/73307a9c0c1b26c4820751c27d0e652e45bfc8f3/images/dashboard.png

---

# Author

**Altin Salihi**

Data Analyst | Business Intelligence Analyst

Microsoft Fabric | Power BI | Python | SQL
