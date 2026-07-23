#!/usr/bin/env python
# coding: utf-8

# ## 05_Portfolio_Assistant
# 
# null

# In[4]:


from pyspark.sql import functions as F
from datetime import datetime

print("=" * 70)
print("PERSONALIZED AI PORTFOLIO ASSISTANT")
print("=" * 70)
print("Started at:", datetime.now())

# =========================================================
# 1. LOAD MARKET INTELLIGENCE
# =========================================================

market_intelligence_df = (
    spark.table("ticker_market_intelligence")
    .withColumn(
        "Ticker",
        F.upper(F.trim(F.col("Ticker")))
    )
)

print(
    "Market intelligence tickers available:",
    market_intelligence_df.count()
)

# =========================================================
# 2. LOAD REAL PORTFOLIO POSITIONS
# =========================================================

portfolio_positions_source_df = (
    spark.table("portfolio_positions")
    .withColumn(
        "Ticker",
        F.upper(F.trim(F.col("Ticker")))
    )
)

print("Portfolio positions schema:")
portfolio_positions_source_df.printSchema()

portfolio_positions_df = (
    portfolio_positions_source_df
    .select(
        "Ticker",

        F.col("Current_Shares")
        .cast("double")
        .alias("Current_Shares"),

        F.col("Average_Cost")
        .cast("double")
        .alias("Average_Cost"),

        F.col("Current_Price")
        .cast("double")
        .alias("Current_Price"),

        F.col("Market_Value")
        .cast("double")
        .alias("Market_Value"),

        F.col("Remaining_Cost_Basis")
        .cast("double")
        .alias("Remaining_Cost_Basis"),

        F.col("Unrealized_Profit")
        .cast("double")
        .alias("Unrealized_Profit"),

        F.col("Unrealized_Return_%")
        .cast("double")
        .alias("Unrealized_Return_Pct")
    )
    .dropDuplicates(["Ticker"])
)

print(
    "Portfolio ticker rows:",
    portfolio_positions_df.count()
)

# =========================================================
# 3. CALCULATE SENTIMENT PERCENTAGES
# =========================================================

assistant_base_df = (
    market_intelligence_df

    .withColumn(
        "Positive_Percentage",
        F.round(
            F.col("Positive_Articles")
            / F.col("Articles_Analyzed") * 100,
            2
        )
    )

    .withColumn(
        "Neutral_Percentage",
        F.round(
            F.col("Neutral_Articles")
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
)

# =========================================================
# 4. OVERALL SENTIMENT
# =========================================================

assistant_base_df = (
    assistant_base_df
    .withColumn(
        "Overall_Sentiment",

        F.when(
            F.col("Average_Sentiment_Score") >= 0.20,
            "Strongly Positive"
        )
        .when(
            F.col("Average_Sentiment_Score") >= 0.05,
            "Positive"
        )
        .when(
            F.col("Average_Sentiment_Score") > -0.05,
            "Neutral"
        )
        .when(
            F.col("Average_Sentiment_Score") > -0.20,
            "Negative"
        )
        .otherwise("Strongly Negative")
    )
)

# =========================================================
# 5. RISK LEVEL
# =========================================================

assistant_base_df = (
    assistant_base_df
    .withColumn(
        "Assistant_Risk_Level",

        F.when(
            (F.col("Negative_Percentage") >= 45)
            | (F.col("Average_Sentiment_Score") <= -0.20),
            "High"
        )
        .when(
            (F.col("Negative_Percentage") >= 30)
            | (F.col("Average_Sentiment_Score") < -0.05),
            "Medium"
        )
        .otherwise("Low")
    )
)

# =========================================================
# 6. AI SIGNAL
# =========================================================

assistant_base_df = (
    assistant_base_df
    .withColumn(
        "Assistant_Signal",

        F.when(
            (F.col("Average_Sentiment_Score") >= 0.20)
            & (F.col("Positive_Percentage") >= 45)
            & (F.col("Articles_Analyzed") >= 10),
            "Positive Watch"
        )
        .when(
            (F.col("Average_Sentiment_Score") >= 0.05)
            & (
                F.col("Positive_Percentage")
                > F.col("Negative_Percentage")
            ),
            "Constructive"
        )
        .when(
            (F.col("Average_Sentiment_Score") <= -0.20)
            | (F.col("Negative_Percentage") >= 45),
            "Caution"
        )
        .when(
            F.col("Average_Sentiment_Score") < -0.05,
            "Watch Risk"
        )
        .otherwise("Hold / Monitor")
    )
)

# =========================================================
# 7. EVIDENCE STRENGTH
# =========================================================

assistant_base_df = (
    assistant_base_df
    .withColumn(
        "Evidence_Strength",

        F.when(
            F.col("Articles_Analyzed") >= 100,
            "Strong"
        )
        .when(
            F.col("Articles_Analyzed") >= 30,
            "Moderate"
        )
        .otherwise("Limited")
    )
)

# =========================================================
# 8. JOIN AI DATA WITH PORTFOLIO
# =========================================================

personalized_assistant_df = (
    assistant_base_df
    .join(
        portfolio_positions_df,
        on="Ticker",
        how="left"
    )
    .withColumn(
        "Currently_Owned",
        F.when(
            F.coalesce(
                F.col("Current_Shares"),
                F.lit(0.0)
            ) > 0,
            "Yes"
        )
        .otherwise("No")
    )
)

# =========================================================
# 9. POSITION STATUS
# =========================================================

personalized_assistant_df = (
    personalized_assistant_df
    .withColumn(
        "Position_Status",

        F.when(
            F.col("Currently_Owned") == "No",
            "Not Owned"
        )
        .when(
            F.col("Unrealized_Profit") > 0,
            "Unrealized Gain"
        )
        .when(
            F.col("Unrealized_Profit") < 0,
            "Unrealized Loss"
        )
        .otherwise("Break Even")
    )
)

# =========================================================
# 10. PERSONALIZED ACTION
# =========================================================

personalized_assistant_df = (
    personalized_assistant_df
    .withColumn(
        "Personalized_Action",

        F.when(
            F.col("Currently_Owned") == "No",

            F.when(
                F.col("Assistant_Signal").isin(
                    "Constructive",
                    "Positive Watch"
                ),
                F.lit(
                    "You do not currently own this stock. "
                    "The recent news outlook is favorable, but review its "
                    "valuation, fundamentals and effect on portfolio "
                    "diversification before opening a position."
                )
            )
            .when(
                F.col("Assistant_Signal") == "Hold / Monitor",
                F.lit(
                    "You do not currently own this stock. "
                    "The news signal is currently neutral, so keeping it "
                    "on a watchlist may be more appropriate than taking "
                    "immediate action."
                )
            )
            .otherwise(
                F.lit(
                    "You do not currently own this stock. "
                    "Recent news risk is elevated, so avoid opening a new "
                    "position until sentiment and supporting evidence improve."
                )
            )
        )

        .when(
            F.col("Assistant_Signal") == "Caution",

            F.when(
                F.col("Unrealized_Profit") < 0,
                F.lit(
                    "You are currently holding this position at an "
                    "unrealized loss. Consider holding rather than averaging "
                    "down while sentiment remains strongly negative. "
                    "Review the position again if the negative trend continues."
                )
            )
            .otherwise(
                F.lit(
                    "You currently have an unrealized gain, but recent news "
                    "sentiment is unfavorable. Avoid adding more shares and "
                    "consider protecting part of the gain if the position "
                    "exceeds your target allocation."
                )
            )
        )

        .when(
            F.col("Assistant_Signal") == "Watch Risk",

            F.when(
                F.col("Unrealized_Profit") < 0,
                F.lit(
                    "Maintain close monitoring and avoid averaging down "
                    "until the negative news trend stabilizes. Reassess the "
                    "position after stronger earnings or more favorable news."
                )
            )
            .otherwise(
                F.lit(
                    "The position remains profitable, but news risk is rising. "
                    "Continue monitoring it closely and avoid increasing "
                    "exposure until sentiment improves."
                )
            )
        )

        .when(
            F.col("Assistant_Signal") == "Hold / Monitor",

            F.lit(
                "Continue holding and monitoring the position. "
                "The current news flow does not indicate an urgent action. "
                "Pay attention to upcoming earnings, guidance and major "
                "company announcements."
            )
        )

        .when(
            F.col("Assistant_Signal").isin(
                "Constructive",
                "Positive Watch"
            ),

            F.when(
                F.col("Unrealized_Profit") < 0,
                F.lit(
                    "Recent news is favorable while the position remains "
                    "at an unrealized loss. Holding may be reasonable. "
                    "Only consider averaging down gradually if it remains "
                    "consistent with your risk tolerance and allocation limits."
                )
            )
            .otherwise(
                F.lit(
                    "The position is currently supported by favorable news "
                    "and an unrealized gain. Continue holding. Consider adding "
                    "only if the stock remains within your desired portfolio "
                    "allocation and your broader analysis supports it."
                )
            )
        )

        .otherwise(
            F.lit(
                "Continue monitoring the position before making a new decision."
            )
        )
    )
)

# =========================================================
# 11. ASSISTANT REASON
# =========================================================

personalized_assistant_df = (
    personalized_assistant_df
    .withColumn(
        "Assistant_Reason",

        F.concat(
            F.lit("Analyzed "),
            F.col("Articles_Analyzed").cast("string"),
            F.lit(" articles. Positive: "),
            F.col("Positive_Percentage").cast("string"),
            F.lit("%, neutral: "),
            F.col("Neutral_Percentage").cast("string"),
            F.lit("%, negative: "),
            F.col("Negative_Percentage").cast("string"),
            F.lit("%. Average sentiment score: "),
            F.round(
                F.col("Average_Sentiment_Score"),
                3
            ).cast("string"),
            F.lit(". Main news topic: "),
            F.coalesce(
                F.col("Primary_Topic"),
                F.lit("unclassified")
            ),
            F.lit(".")
        )
    )
)

# =========================================================
# 12. FORMATTED VALUES
# =========================================================

personalized_assistant_df = (
    personalized_assistant_df

    .withColumn(
        "Formatted_Unrealized_Profit",

        F.when(
            F.col("Unrealized_Profit") < 0,
            F.concat(
                F.lit("-$"),
                F.format_number(
                    F.abs(F.col("Unrealized_Profit")),
                    2
                )
            )
        )
        .otherwise(
            F.concat(
                F.lit("$"),
                F.format_number(
                    F.coalesce(
                        F.col("Unrealized_Profit"),
                        F.lit(0.0)
                    ),
                    2
                )
            )
        )
    )

    .withColumn(
        "Formatted_Unrealized_Return",

        F.concat(
            F.format_number(
                F.col("Unrealized_Return_Pct") * 100,
                2
            ),
            F.lit("%")
        )
    )
)

# =========================================================
# 13. PERSONALIZED ASSISTANT RESPONSE
# =========================================================

personalized_assistant_df = (
    personalized_assistant_df
    .withColumn(
        "Assistant_Response",

        F.when(
            F.col("Currently_Owned") == "Yes",

            F.concat(
                F.lit("You currently own "),
                F.format_number(
                    F.col("Current_Shares"),
                    4
                ),
                F.lit(" shares of "),
                F.col("Ticker"),

                F.lit(". Your average purchase price is $"),
                F.format_number(
                    F.col("Average_Cost"),
                    2
                ),

                F.lit(", while the current price is $"),
                F.format_number(
                    F.col("Current_Price"),
                    2
                ),

                F.lit(". Your current market value is $"),
                F.format_number(
                    F.col("Market_Value"),
                    2
                ),

                F.lit(". The position has an unrealized profit/loss of "),
                F.col("Formatted_Unrealized_Profit"),

                F.lit(" ("),
                F.col("Formatted_Unrealized_Return"),
                F.lit(")."),

                F.lit(" Recent news sentiment is "),
                F.lower(F.col("Overall_Sentiment")),

                F.lit(" ("),
                F.format_number(
                    F.col("Average_Sentiment_Score") * 100,
                    2
                ),
                F.lit("%), based on "),

                F.col("Articles_Analyzed").cast("string"),
                F.lit(" analyzed articles. "),

                F.format_number(
                    F.col("Positive_Percentage"),
                    2
                ),
                F.lit("% were positive and "),

                F.format_number(
                    F.col("Negative_Percentage"),
                    2
                ),
                F.lit("% were negative."),

                F.lit(" The dominant news topic is "),
                F.coalesce(
                    F.col("Primary_Topic"),
                    F.lit("unclassified")
                ),

                F.lit(". Current AI recommendation: "),
                F.col("Assistant_Signal"),

                F.lit(", with "),
                F.lower(F.col("Assistant_Risk_Level")),
                F.lit(" news-related risk. "),

                F.col("Personalized_Action"),

                F.lit(" AI classification confidence is "),
                F.format_number(
                    F.col("Average_Sentiment_Confidence") * 100,
                    0
                ),
                F.lit("%."),

                F.lit(
                    " This is decision-support analysis, "
                    "not guaranteed financial advice."
                )
            )
        )

        .otherwise(
            F.concat(
                F.lit("You do not currently own "),
                F.col("Ticker"),

                F.lit(". Recent news sentiment is "),
                F.lower(F.col("Overall_Sentiment")),

                F.lit(" ("),
                F.format_number(
                    F.col("Average_Sentiment_Score") * 100,
                    2
                ),
                F.lit("%), based on "),

                F.col("Articles_Analyzed").cast("string"),
                F.lit(" analyzed articles. "),

                F.format_number(
                    F.col("Positive_Percentage"),
                    2
                ),
                F.lit("% were positive and "),

                F.format_number(
                    F.col("Negative_Percentage"),
                    2
                ),
                F.lit("% were negative."),

                F.lit(" The dominant topic is "),
                F.coalesce(
                    F.col("Primary_Topic"),
                    F.lit("unclassified")
                ),

                F.lit(". Current AI recommendation: "),
                F.col("Assistant_Signal"),

                F.lit(", with "),
                F.lower(F.col("Assistant_Risk_Level")),
                F.lit(" news-related risk. "),

                F.col("Personalized_Action"),

                F.lit(" AI classification confidence is "),
                F.format_number(
                    F.col("Average_Sentiment_Confidence") * 100,
                    0
                ),
                F.lit("%."),

                F.lit(
                    " This is decision-support analysis, "
                    "not guaranteed financial advice."
                )
            )
        )
    )

    .withColumn(
        "Assistant_Generated_At",
        F.current_timestamp()
    )
)

# =========================================================
# 14. DISPLAY VALIDATION
# =========================================================

display(
    personalized_assistant_df.select(
        "Ticker",
        "Currently_Owned",
        "Current_Shares",
        "Average_Cost",
        "Current_Price",
        "Market_Value",
        "Unrealized_Profit",
        "Unrealized_Return_Pct",
        "Formatted_Unrealized_Profit",
        "Formatted_Unrealized_Return",
        "Overall_Sentiment",
        "Assistant_Signal",
        "Assistant_Risk_Level",
        "Average_Sentiment_Confidence",
        "Assistant_Response"
    ).orderBy(
        F.desc("Average_Sentiment_Score")
    )
)

# =========================================================
# 15. SAVE UPDATED TABLE
# =========================================================

(
    personalized_assistant_df
    .write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .format("delta")
    .saveAsTable("portfolio_assistant_signals")
)

print(
    "Table portfolio_assistant_signals "
    "saved successfully."
)

# =========================================================
# 16. FINAL VALIDATION
# =========================================================

saved_assistant_df = spark.table(
    "portfolio_assistant_signals"
)

assistant_count = saved_assistant_df.count()

owned_count = (
    saved_assistant_df
    .filter(F.col("Currently_Owned") == "Yes")
    .count()
)

print("=" * 70)
print("PERSONALIZED PORTFOLIO ASSISTANT COMPLETED SUCCESSFULLY")
print("=" * 70)
print("Ticker responses created:", assistant_count)
print("Owned portfolio positions matched:", owned_count)
print("Input AI table: ticker_market_intelligence")
print("Input portfolio table: portfolio_positions")
print("Output table: portfolio_assistant_signals")
print("Lakehouse: AI_Financial_Lakehouse")

