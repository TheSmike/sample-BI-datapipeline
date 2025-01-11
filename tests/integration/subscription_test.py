
from subscription_pipeline.jobs.staging2silver import SubscriptionStagingToSilver
import sys

def test_dataframe_operations(spark_session):
    print(sys.path)
    sut = SubscriptionStagingToSilver(spark_session, "tests/samples/subscriptions/", "tmp/test/output/subscription/", "test")
    result_df = sut._compute_df("2024-01", "2024-09")
    result_df.show()
    # assert e.g., result_df == expected
