
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

from subscription_pipeline.date_check import validate_month_format


class SubscriptionSilver2Gold():
    """
    This Job extract data from `subscription.subscription_event` and create 3 new tables:
    - subscription_monthly_status (fact)
    - subscription (dim) 
    - customer (dim)
    """

    def __init__(
        self,
        spark: SparkSession,
        env: str,
        root_output_path: str,
        # we could parametrize also output table names
    ):
        self.spark = spark
        self.env = env
        self.root_output_path = root_output_path
        
        
    def run(self, from_month: str, to_month: str):
        
        self._validate_input(from_month, to_month)
        
        event_df = self.spark.read.table(f"{self.env}.subscription.subscription_event")
        event_df = event_df.filter(f"year_month BETWEEN '{from_month}' AND '{to_month}'")
        event_df.cache()
        event_df.createOrReplaceTempView("filtered_event")

        self._upsert_customer_table()
        self._upsert_subscription_table(event_df)
        self._write_new_subscription_monthly_status(event_df, from_month, to_month)


    def _validate_input(self, from_month: str, to_month: str):
        validate_month_format(from_month)
        validate_month_format(to_month)


    def _upsert_customer_table(self):
        # Customer table doesn't contain mutable fields; thus, we insert only new customers if present
        self.spark.sql(f"""
        MERGE INTO {self.env}.subscription.customer_dim t
            USING {self.env}.subscription.filtered_event n
            ON t.customer_id = n.customer_id
            WHEN NOT MATCHED THEN
                INSERT ( customer_id, registration_ts, registration_country )
                VALUES ( n.customer_id, n.customer_registration_country, n.customer_registration_ts )
        """)
        # In alternative, an overwrite strategy could be evaluated if it's more performant for the size of this table (expected to be small)


    def _upsert_subscription_table(self, event_df: DataFrame):
        # -- SUBSCRIPTION TABLE --
        last_subscription_df = self.spark.read.table(f"{self.env}.subscription.subscription_dim").where("valid_to IS NULL")

        event_subscriptions_df = event_df.selectExpr( 
            "subscription_id",
            "subscription_category as category",
            "subscription_category_variant as category_variant",
            "subscription_creations_ts as creation_ts",
        )

        joined_sub_df = last_subscription_df.alias("last").join(
            event_subscriptions_df.alias("ev"),
            on=F.expr("last.subscription_id = ev.subscription_id"),
            how="full"
        ).cache()

        sub_scd_df = (
            joined_sub_df
            .where("last.subscription_id IS NOT NULL AND ev.subscription_id IS NOT NULL")
            .where("last.category <> new.category OR subscription_category_variant as category_variant")
        )
        sub_scd_df.createOrReplaceTempView("sub_scd")

        # close SCD setting valid_to
        self.spark.sql(f"""
        UPDATE {self.env}.subscription.subscription_dim
        SET t.valid_to = NOW()
        WHERE subscription_id IN (select subscription_id from sub_scd)
        """)

        # compute new records for the subscription dim table, it' composed by new subscription and old subscription with changes (e.g., chanfe of category)
        new_sub_records_df = (
            joined_sub_df
            .where("last.subscription_id IS NULL")
            .union(sub_scd_df)
            .select(
                "ev.subscription_id",
                "ev.category",
                "ev.category_variant"
            )
        )

        # insert new records
        (
            new_sub_records_df.write
            .format("delta")
            .mode("append")
            .saveAsTable(f"{self.env}.subscription.subscription_dim")
        )

        # alternative: if you can't afford a moment of inconsistency (a subscription without a NULL valid_to), the correct approach is this in a delta lake based lakehouse:
        # https://medium.com/@smdbilal.vt5815/delta-lake-the-game-changer-for-slowly-changing-dimensions-a-step-by-step-guide-to-scd-type-2-ab326b986671
        # if you're in a OLAP system like Redshift, you could use TRANSACTIONS instead


    def _write_new_subscription_monthly_status(self, event_df: DataFrame, from_month: str, to_month: str):
        # This is a fact table and it conceptually contains "events" so we have to just add new records, however you can rewrite also old data 
        # in blocks identified by the year_month field (e.g., the algorithm is changed or a bug was founded)
        (
            event_df.write
            .mode("overwrite")
            .option("replaceWhere", f"year_month >= '{from_month}' AND year_month <= '{to_month}'")
            .partitionBy("year")
            .saveAsTable(f"{self.env}.subscription.subscription_monthly_status")
        )
