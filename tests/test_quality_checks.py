from scripts.quality_checks.validate_data import calculate_quality_score


def test_quality_score_full():
    checks = {
        "nulls": 0,
        "duplicates": 0,
        "referential_issues": 0,
        "range_issues": 0
    }
    score = calculate_quality_score(checks)
    assert score == 100.0


def test_quality_score_partial():
    checks = {
        "nulls": 1,
        "duplicates": 1,
        "referential_issues": 0,
        "range_issues": 0
    }
    score = calculate_quality_score(checks)
    assert score < 100.0
