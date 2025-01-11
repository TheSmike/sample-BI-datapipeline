from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from subscription_pipeline.date_check import validate_month_format


class SubscriptionStagingToSilver():

    def __init__(
        self,
        spark: SparkSession,
        staging_path: str,
        root_output_path: str,
        env: str,
    ):
        self.spark = spark
        self.staging_path = staging_path
        self.root_output_path = root_output_path
        self.env = env

    def run(self, from_month: str, to_month: str): 
        
        self._validate_input(from_month, to_month)
        
        df = self._compute_df(from_month, to_month)
        
        self._write_df(df, from_month, to_month)
        
    
    def _validate_input(self, from_month: str, to_month: str):
        validate_month_format(from_month)
        validate_month_format(to_month)

    def _compute_df(self, from_month: str, to_month: str):
        df = self.spark.read.json(self.staging_path, multiLine=True)

        df = df.withColumn("year_month", F.concat(df.year, F.lit("-"), F.lpad(df.month, 2, "0") ))

        df = df.filter(f"year_month BETWEEN '{from_month}' AND '{to_month}'")

        # For the scope of the exercise, we push down in the pipeline only the fields used downstream in the exercise.
        # However, in the medallion lakehouse architecture, we should push down everything useful about the event for further analysis.

        df =  df.selectExpr(
            "id as event_id",
            "year_month",
            "year",
            "month",
            "TO_TIMESTAMP(timestamp) as event_ts",
            "data.user.id as customer_id",
            "data.user.registration.country as customer_registration_country",
            "TO_TIMESTAMP(data.user.createdAt) as customer_registration_ts",
            "data.user.subscription.id as subscription_id",
            "data.user.subscription.categoryVariant as subscription_category_variant",
            "data.user.subscription.category as subscription_category",
            "TO_TIMESTAMP(data.user.subscription.createdAt) as subscription_creations_ts",
            "CAST(data.user.subscription.billing.cycle.price.full as DECIMAL(9,2)) as full_price",
            "CAST(data.user.subscription.billing.cycle.price.net  as DECIMAL(9,2)) as netl_price",
            "CAST(data.user.subscription.billing.cycle.price.vat  as DECIMAL(9,2)) as vat_price",
            "data.user.subscription.status.renew as renewal",
            "data.user.subscription.status.suspend as suspended",
            "TO_TIMESTAMP(data.cycle.createdAt) as cycle_creation_ts",
            "TO_TIMESTAMP(data.cycle.expiredAt) as cycle_expiration_ts",
            # ... a lot of other things
        )
        
        # In this phase, we should also insert DQ checks. e.g., we could check a set of rules and quarantine records that doesn't respect those rules
        
        # df = check_DQ_rules()
        return df
        

    def _write_df(self, df, from_month: str, to_month: str):
        # we don't expect too many records here so, we don't apply partitiong
        (
            df.write
            .format("delta")
            .mode("overwrite")
            .option("replaceWhere", f"year_month >= '{from_month}' AND year_month <= '{to_month}'")
            .saveAsTable(
                f"{self.env}.subscription.subscription_event",
                path=f"{self.root_output_path}/subscription_events/"
            )
        )
        
        
