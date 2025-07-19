# SPARK JOB FOR RETAIL BEHAVIORAL ANALYSIS (MBA & RFM)

from pyspark.sql import SparkSession
from pyspark.sql.functions import (col, sum as _sum, when, collect_set, datediff, 
                                   lit, max as _max, countDistinct, explode, first, size, ntile)
from pyspark.ml.fpm import FPGrowth
from pyspark.sql.window import Window

def run_spark_job():
    """
    Main function to run the Spark job.
    - Processes all daily CSV files from generated_data.
    - Performs Market Basket Analysis (MBA) to find product associations.
    - Performs RFM Analysis to segment customers.
    - Saves both analytical results to PostgreSQL.
    """
    # --- 1. Initialize Spark Session ---
    spark = SparkSession.builder \
        .appName("RetailBehavioralAnalytics") \
        .master("local[*]") \
        .getOrCreate()
    
    print("Spark Session created successfully")

    # --- 2. Load Data ---
    data_path = "/opt/bitnami/spark/data/*.csv" 
    df = spark.read.csv(data_path, header=True, inferSchema=True)
    print(f"Total records loaded: {df.count()}")

    # --- 3. Data Cleaning ---
    print("Cleaning data...")
    cleaned_df = df.filter(col("Price") > 0) \
                   .filter(col("StockCode").isNotNull()) \
                   .filter(col("StockCode").rlike(".*[0-9].*")) \
                   .filter(col("Description").isNotNull())
    
    product_lookup = cleaned_df.select("StockCode", "Description").distinct()
    
    print(f"Records after cleaning: {cleaned_df.count()}")
    cleaned_df.cache()

    # --- 4. Market Basket Analysis with Product Names ---
    print("Performing Market Basket Analysis...")
    baskets_df = cleaned_df.groupBy("Invoice").agg(collect_set("StockCode").alias("items"))
    baskets_df = baskets_df.filter(size(col("items")) >= 2)
    
    fpGrowth = FPGrowth(itemsCol="items", minSupport=0.005, minConfidence=0.05)
    model = fpGrowth.fit(baskets_df)
    association_rules = model.associationRules

    # Join with product lookup to get antecedent and consequent names
    mba_with_antecedent_names = association_rules.withColumn("antecedent_item", explode(col("antecedent"))) \
        .join(product_lookup, col("antecedent_item") == product_lookup.StockCode) \
        .groupBy(association_rules.columns) \
        .agg(collect_set("Description").alias("antecedent_names"))

    mba_final_results = mba_with_antecedent_names.withColumn("consequent_item", explode(col("consequent"))) \
        .join(product_lookup, col("consequent_item") == product_lookup.StockCode) \
        .groupBy(mba_with_antecedent_names.columns) \
        .agg(collect_set("Description").alias("consequent_names"))
    
    print("Market Basket Analysis complete. Sample association rules with names:")
    mba_final_results.show(5, truncate=False)

    # --- 5. Dynamic RFM Analysis & Segmentation ---
    print("Performing RFM Analysis...")
    df_with_total_price = cleaned_df.withColumn("TotalPrice", col("Quantity") * col("Price"))
    snapshot_date = df_with_total_price.agg(_max(col("InvoiceDate"))).first()[0]

    rfm_calculated_df = df_with_total_price.groupBy("Customer ID").agg(
        datediff(lit(snapshot_date), _max(col("InvoiceDate"))).alias("Recency"),
        countDistinct("Invoice").alias("Frequency"),
        _sum("TotalPrice").alias("Monetary")
    ).filter(col("Customer ID").isNotNull())

    r_window = Window.orderBy(col("Recency").desc())
    f_window = Window.orderBy(col("Frequency"))
    m_window = Window.orderBy(col("Monetary"))

    rfm_with_scores = rfm_calculated_df.withColumn("r_score", ntile(5).over(r_window)) \
                                       .withColumn("f_score", ntile(5).over(f_window)) \
                                       .withColumn("m_score", ntile(5).over(m_window))

    rfm_final_df = rfm_with_scores.withColumn("customer_segment",
        when((col("r_score") >= 4) & (col("f_score") >= 4), "Champions")
        .when((col("r_score") >= 4) & (col("f_score") >= 2), "Potential Loyalists")
        .when((col("r_score") >= 3) & (col("f_score") >= 3), "Loyal Customers")
        .when((col("r_score") <= 2) & (col("f_score") >= 3), "At Risk")
        .when((col("r_score") >= 3) & (col("f_score") <= 2), "New Customers")
        .otherwise("Needs Attention")
    )
    
    print(f"RFM analysis completed for {rfm_final_df.count()} customers. Sample segments:")
    rfm_final_df.show(5)
    
    # --- 6. Save Results to PostgreSQL ---
    print("Saving results to PostgreSQL...")
    
    postgres_properties = {
        "user": "airflow",
        "password": "airflow",
        "driver": "org.postgresql.Driver"
    }
    
    # FIX: Corrected hostname to 'postgres_db' to match the service name in docker-compose
    postgres_url = "jdbc:postgresql://postgres_db:5432/airflow" 
    
    # FIX: Save the DataFrame that includes product names
    mba_final_results.select("antecedent_names", "consequent_names", "confidence", "lift", "support") \
        .write \
        .mode("overwrite") \
        .jdbc(postgres_url, "market_basket_analysis", properties=postgres_properties)
    
    # FIX: Save the DataFrame that includes the final segment names
    rfm_final_df.select("Customer ID", "Recency", "Frequency", "Monetary", "customer_segment") \
        .write \
        .mode("overwrite") \
        .jdbc(postgres_url, "customer_segmentation", properties=postgres_properties)
    
    print("Results saved successfully!")
    
    spark.stop()

if __name__ == "__main__":
    run_spark_job()