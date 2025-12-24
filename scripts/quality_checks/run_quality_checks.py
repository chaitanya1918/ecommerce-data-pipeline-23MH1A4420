import psycopg2
import json

from validate_data import (
    check_null_values,
    check_duplicates,
    check_referential_integrity,
    check_data_ranges,
    calculate_quality_score
)

connection = psycopg2.connect(
    host="host.docker.internal",
    database="postgres",
    user="postgres",
    password="Chaituram@1918"
)

results = {}
results["nulls"] = check_null_values(connection, "staging")
results["duplicates"] = check_duplicates(connection, "staging")
results["referential_issues"] = check_referential_integrity(connection, "staging")
results["range_issues"] = check_data_ranges(connection, "staging")
results["quality_score"] = calculate_quality_score(results)

connection.close()

with open("data/staging/quality_report.json", "w") as f:
    json.dump(results, f, indent=4)

print("QUALITY CHECK COMPLETED")
print("Report saved to: data/staging/quality_report.json")
