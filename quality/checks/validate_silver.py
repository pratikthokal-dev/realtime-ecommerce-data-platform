import sys
import pandas as pd
import great_expectations as gx


def run_quality_checks():

    # Load Silver data
    silver_df = pd.read_parquet(
        "data/silver/orders"
    )

    context = gx.get_context()

    data_source = context.data_sources.add_pandas(
        name="silver_quality_source"
    )

    data_asset = data_source.add_dataframe_asset(
        name="silver_orders"
    )

    batch_definition = data_asset.add_batch_definition_whole_dataframe(
        "silver_batch"
    )

    batch = batch_definition.get_batch(
        batch_parameters={
            "dataframe": silver_df
        }
    )

    expectations = [
        gx.expectations.ExpectColumnValuesToNotBeNull(
            column="order_id"
        ),
        gx.expectations.ExpectColumnValuesToNotBeNull(
            column="customer_id"
        ),
        gx.expectations.ExpectColumnValuesToNotBeNull(
            column="amount"
        ),
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="amount",
            min_value=0
        ),
        gx.expectations.ExpectColumnValuesToBeInSet(
            column="status",
            value_set=[
                "PLACED",
                "CONFIRMED",
                "SHIPPED",
                "DELIVERED",
                "CANCELLED"
            ]
        ),
        gx.expectations.ExpectColumnValuesToNotBeNull(
            column="timestamp"
        ),
        gx.expectations.ExpectColumnValuesToNotBeNull(
            column="event_date"
        )
    ]

    passed = 0
    failed = 0

    print("\n======================================")
    print(" SILVER DATA QUALITY VALIDATION")
    print("======================================")

    for expectation in expectations:

        result = batch.validate(expectation)

        name = expectation.__class__.__name__

        if result["success"]:
            print(f"✅ PASS : {name}")
            passed += 1
        else:
            print(f"❌ FAIL : {name}")
            failed += 1

    print("\n======================================")
    print(" VALIDATION SUMMARY")
    print("======================================")

    print(f"Total Checks : {len(expectations)}")
    print(f"Passed       : {passed}")
    print(f"Failed       : {failed}")

    if failed == 0:
        print("\n🎉 ALL DATA QUALITY CHECKS PASSED")
        return True

    print("\n❌ DATA QUALITY CHECKS FAILED")
    return False


if __name__ == "__main__":

    success = run_quality_checks()

    if not success:
        sys.exit(1)