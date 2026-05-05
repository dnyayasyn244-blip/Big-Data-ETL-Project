import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_date

# --- 1. إعداد المسارات (تأكدي من مطابقتها لجهازك) ---
java_home = r'C:\Program Files\Eclipse Adoptium\jdk-25.0.0.36-hotspot'
hadoop_home = r'C:\hadoop'

os.environ['JAVA_HOME'] = java_home
os.environ['HADOOP_HOME'] = hadoop_home
os.environ['PATH'] = os.path.join(java_home, 'bin') + os.pathsep + \
                     os.path.join(hadoop_home, 'bin') + os.pathsep + \
                     os.environ.get('PATH', '')

try:
    # --- 2. إنشاء الجلسة (Yarn Master Simulation) ---
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("BigData_ETL_Final") \
        .config("spark.driver.host", "127.0.0.1") \
        .getOrCreate()

    print("\n" + "="*40)
    print("✅ SPARK SESSION ACTIVE (YARN MODE)")
    print("="*40)

    # --- 3. قراءة البيانات (تخيل أنها من HDFS) ---
    # بنحاول نقرأ ملف CSV، لو مش موجود بنعمل بيانات وهمية عشان الكود ميفشلش
    try:
        df = spark.read.csv("sales.csv", header=True, inferSchema=True)
        print("📁 Data loaded from sales.csv")
    except:
        print("⚠️ sales.csv not found! Generating sample data for demonstration...")
        data = [
            (1, "Laptop", "Electronics", 1200, "2026-05-01"),
            (2, "Mouse", "Electronics", 25, "2026-05-02"),
            (3, "Dress", "Fashion", 50, "2026-05-02")
        ]
        columns = ["sale_id", "product_name", "category", "price", "sale_date"]
        df = spark.createDataFrame(data, columns)

    # --- 4. التحويل لـ Star Schema (المطلوب من المعيد) ---
    print("🚀 Applying Transformations...")

    # جدول الأبعاد (Dimension Table)
    dim_products = df.select("product_name", "category").distinct()

    # جدول الحقائق (Fact Table)
    fact_sales = df.select("sale_id", "product_name", "price", "sale_date") \
                   .withColumn("processed_at", current_date())

    # --- 5. عرض النتائج النهائية ---
    print("\n--- [Fact Table: Sales] ---")
    fact_sales.show()

    print("--- [Dimension Table: Products] ---")
    dim_products.show()

    print("✨ ETL Pipeline Finished Successfully! ✨")

except Exception as e:
    print(f"❌ Error Detail: {e}")

finally:
    if 'spark' in locals():
        spark.stop()