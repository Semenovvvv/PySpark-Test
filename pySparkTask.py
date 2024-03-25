from pyspark.sql import SparkSession
from pyspark.sql.functions import col


def get_product_category_pairs_with_null_categories(df):
    product_category_pairs = df.select("product_name", "category_name")
    
    products_with_null_categories = df.filter(col("category_name").isNull()).select("product_name").distinct()
    
    result = product_category_pairs.union(products_with_null_categories.selectExpr("product_name", "NULL as category_name"))
    
    return result


def main():
    spark = SparkSession.builder \
        .appName("ProductCategoryPairs") \
        .getOrCreate()

    result_df = get_product_category_pairs_with_null_categories(df)
    result_df.show()

    spark.stop()


if __name__ == "__main__":
    main()