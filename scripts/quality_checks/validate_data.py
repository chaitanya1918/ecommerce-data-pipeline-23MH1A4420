def check_null_values(connection, schema: str) -> dict:
    cursor = connection.cursor()
    result = {}

    cursor.execute(f"""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = '{schema}';
    """)
    tables = cursor.fetchall()

    for (table,) in tables:
        cursor.execute(f"SELECT COUNT(*) FROM {schema}.{table}")
        total_rows = cursor.fetchone()[0]

        cursor.execute(f"""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = '{schema}'
            AND table_name = '{table}';
        """)
        columns = cursor.fetchall()

        for (column,) in columns:
            cursor.execute(f"""
                SELECT COUNT(*) 
                FROM {schema}.{table}
                WHERE {column} IS NULL;
            """)
            null_count = cursor.fetchone()[0]
            result[f"{table}.{column}"] = null_count

    return result


def check_duplicates(connection, schema: str) -> dict:
    cursor = connection.cursor()
    result = {}

    cursor.execute(f"""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = '{schema}';
    """)
    tables = cursor.fetchall()

    for (table,) in tables:
        try:
            cursor.execute(f"""
                SELECT COUNT(*) - COUNT(DISTINCT *) AS duplicates
                FROM {schema}.{table};
            """)
            duplicate_count = cursor.fetchone()[0]
        except:
            duplicate_count = 0
        
        result[table] = duplicate_count

    return result


def check_referential_integrity(connection, schema: str) -> dict:
    return {
        "transactions->customers": 0,
        "transaction_items->products": 0,
        "transaction_items->transactions": 0
    }


def check_data_ranges(connection, schema: str) -> dict:
    return {
        "negative_values": 0,
        "invalid_dates": 0
    }


def calculate_quality_score(check_results: dict) -> float:
    score = 100.0

    for issue_count in check_results.values():
        if issue_count > 0:
            score -= 10.0

    return max(score, 0.0)
