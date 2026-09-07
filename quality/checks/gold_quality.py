import sys
import pandas as pd
import great_expectations as gx


def run_gold_quality_checks():

    # Load Gold data
    gold_df = pd.read_parquet(
        "data/gold/daily_sales"
    )

    context = gx.get_context()

    data_source = context.data_sources.add_pandas(
        name="gold_quality_source"
    )

    data_asset = data_source.add_dataframe_asset(
        name="gold_daily_sales"
    )

    batch_definition = data_asset.add_batch_definition_whole_dataframe(
        "gold_batch"
    )

    batch = batch_definition.get_batch(
        batch_parameters={
            "dataframe": gold_df
        }
    )

    # Gold data quality expectations
    expectations = [
        gx.expectations.ExpectColumnValuesToNotBeNull(
            column="event_date"
        ),

        gx.expectations.ExpectColumnValuesToBeBetween(
            column="total_orders",
            min_value=1
        ),

        gx.expectations.ExpectColumnValuesToBeBetween(
            column="total_revenue",
            min_value=0
        ),

        gx.expectations.ExpectColumnValuesToBeBetween(
            column="average_order_value",
            min_value=0
        )
    ]

    passed = 0
    failed = 0

    print("\n======================================")
    print(" GOLD DATA QUALITY VALIDATION")
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
        print("\n🎉 ALL GOLD DATA QUALITY CHECKS PASSED")
        return True

    print("\n❌ GOLD DATA QUALITY CHECKS FAILED")
    return False


if __name__ == "__main__":

    success = run_gold_quality_checks()

    if not success:
        sys.exit(1)