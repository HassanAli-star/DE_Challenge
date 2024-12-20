import unittest
import pandas as pd
import sys
import os
from etl.transformation import *

class TestETL(unittest.TestCase):
    """
    Unit tests for the ETL transformation functions.
    """

    def setUp(self):
        """
        Prepare sample input and expected output for tests.
        """
        # Sample input and expected output for clients
        self.clients_input = [
            {
                "_id.$oid": "6086c347701bfd9e246ae133",
                "name": "John Doe",
                "contract_start.$date": "2023-01-01",
                "contract_end.$date": "2023-12-31",
                "sonar_dates": ["2023-02-01", "2023-05-01"],
                "suppliers": ["Supplier1", "Supplier2"]
            },
            {
                "_id.$oid": "6086c347701bfd9e246ae133",
                "name": "Jane Smith",
                "contract_start.$date": "2023-02-01",
                "contract_end.$date": "2023-11-30",
                "sonar_dates": ["2023-03-01", "2023-06-01"],
                "suppliers": ["Supplier3"]
            },
        ]
        self.clients_expected = pd.DataFrame(
            {
                "client_id": ["6086c347701bfd9e246ae133", "6086c347701bfd9e246ae133"],
                "name": ["John Doe", "Jane Smith"],
                "contract_start_date": ["2023-01-01", "2023-02-01"],
                "contract_end_date": ["2023-12-31", "2023-11-30"],
                "sonar_dates": [["2023-02-01", "2023-05-01"], ["2023-03-01", "2023-06-01"]],
                "suppliers": [["Supplier1", "Supplier2"], ["Supplier3"]],
            }
        )

        # Sample input and expected output for suppliers
        self.suppliers_input = [
            {
                "_id.$oid": "6086c347701bfd9e246ae133",
                "name": "Supplier A",
                "country": "USA",
                "page_status": "Active",
                "login": True,
                "automatic_priority": 1.0,
                "alias": "SupA",
                "date": "2023-01-01",
                "priority": 2.0,
                "currency": "USD",
            }
        ]
        self.suppliers_expected = pd.DataFrame(
            {
                "supplier_id": ["6086c347701bfd9e246ae133"],
                "name": ["Supplier A"],
                "country": ["USA"],
                "page_status": ["Active"],
                "login": [True],
                "automatic_priority": [1.0],
                "alias": ["SupA"],
                "date": ["2023-01-01"],
                "priority": [2.0],
                "currency": ["USD"],
            }
        )

        # Sample input and expected output for supplier_group
        self.supplier_group_input = [
            {
                "client_id": {"$oid": "6086c347701bfd9e246ae133"},
                "supplier_groups": {
                    "group1": [{"$oid": "101"}, {"$oid": "102"}],
                    "group2": [{"$oid": "103"}]
                }
            }
        ]
        self.supplier_group_expected = pd.DataFrame(
            {
                "client_id": ["6086c347701bfd9e246ae133", "6086c347701bfd9e246ae133", "6086c347701bfd9e246ae133"],
                "group_name": ["group1", "group1", "group2"],
                "supplier_id": ["101", "102", "103"]
            }
        )

        # Sample input and expected output for sonar_runs
        self.sonar_runs_input = [
            {
                "_id.$oid": "6086c347701bfd9e246ae133",
                "client_id.$oid": "6086c347701bfd9e246ae133",
                "countries": ["US", "UK"],
                "supplier_ids": [{"$oid": "301"}, {"$oid": "302"}],
                "client_part_ids": [{"$oid": "301"}, {"$oid": "302"}],
                "status": "Completed",
                "category": "Category A",
                "time.$date": "2023-01-01T12:00:00Z",
                "date.$date": "2023-01-01",
            }
        ]
        self.sonar_runs_expected = pd.DataFrame(
            {
                "sonar_run_id": ["6086c347701bfd9e246ae133"],
                "client_id": ["6086c347701bfd9e246ae133"],
                "countries": [["US", "UK"]],
                "supplier_ids": [["301", "302"]],
                "client_part_ids": [["301", "302"]],
                "status": ["Completed"],
                "category": ["Category A"],
                "sonar_run_time": ["2023-01-01T12:00:00Z"],
                "sonar_run_date": ["2023-01-01"],
            }
        )

        # Sample input and expected output for sonar_results
        self.sonar_results_input = [
            {
                "_id.$oid": "6086c347701bfd9e246ae133",
                "supplier_id.$oid": "6086c347701bfd9e246ae133",
                "sonar_run_id.$oid": "6086c347701bfd9e246ae133",
                "part_id.$oid": "6086c347701bfd9e246ae133",
                "date_sonar.$date": "2023-01-01",
                "date_found.$date": "2023-01-02",
                "price_norm": 100.0,
                "currency": "USD",
                "unit": "kg",
                "country": "USA",
                "status": "Active",
            }
        ]
        self.sonar_results_expected = pd.DataFrame(
            {
                "sonar_result_id": ["6086c347701bfd9e246ae133"],
                "supplier_id": ["6086c347701bfd9e246ae133"],
                "sonar_run_id": ["6086c347701bfd9e246ae133"],
                "part_id": ["6086c347701bfd9e246ae133"],
                "date_sonar": ["2023-01-01"],
                "date_found": ["2023-01-02"],
                "price_norm": [100.0],
                "currency": ["USD"],
                "unit": ["kg"],
                "country": ["USA"],
                "status": ["Active"],
            }
        )

    def test_transform_clients(self):
        obtained_output = TransformationFactory.transform(self.clients_input, case='clients')
        pd.testing.assert_frame_equal(obtained_output, self.clients_expected)
        print("Test for transform_clients passed!")

    def test_transform_suppliers(self):
        obtained_output = TransformationFactory.transform(self.suppliers_input, case='suppliers')
        pd.testing.assert_frame_equal(obtained_output, self.suppliers_expected)
        print("Test for transform_suppliers passed!")

    def test_transform_supplier_group(self):
        obtained_output = TransformationFactory.transform(self.supplier_group_input, case='supplier_group')
        pd.testing.assert_frame_equal(obtained_output, self.supplier_group_expected)
        print("Test for transform_supplier_group passed!")

    def test_transform_sonar_runs(self):
        obtained_output = TransformationFactory.transform(self.sonar_runs_input, case='sonar_runs')
        pd.testing.assert_frame_equal(obtained_output, self.sonar_runs_expected)
        print("Test for transform_sonar_runs passed!")

    def test_transform_sonar_results(self):
        obtained_output = TransformationFactory.transform(self.sonar_results_input, case='sonar_results')
        pd.testing.assert_frame_equal(obtained_output, self.sonar_results_expected)
        print("Test for transform_sonar_results passed!")

if __name__ == "__main__":
    unittest.main()
