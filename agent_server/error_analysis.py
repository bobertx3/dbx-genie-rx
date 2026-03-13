"""Hybrid error analysis for auto-labeling benchmark questions.

Compares Genie's SQL execution results against expected results using
programmatic analysis first, falling back to LLM for ambiguous cases.
"""

import logging
from pydantic import BaseModel

logger = logging.getLogger(__name__)


class ErrorAnalysisResult(BaseModel):
    """Result of comparing two SQL execution results."""
    label: str  # "correct", "incorrect", "inconclusive"
    reason: str
    method: str  # "programmatic" or "llm"


def _normalize_column_name(name: str) -> str:
    """Normalize column name for comparison."""
    return name.lower().strip().replace('"', '').replace('`', '')


def _normalize_value(val) -> str | float | None:
    """Normalize a cell value for comparison."""
    if val is None:
        return None
    if isinstance(val, bool):
        return str(val).lower()
    if isinstance(val, (int, float)):
        return float(val)
    s = str(val).strip()
    if not s or s.lower() in ('none', 'null', 'nan'):
        return None
    # Try parsing as float
    try:
        return float(s)
    except (ValueError, TypeError):
        return s.lower()


def _values_match(a, b, rel_tolerance: float = 0.001, abs_tolerance: float = 0.01) -> bool:
    """Compare two normalized values with tolerance for numerics."""
    if a is None and b is None:
        return True
    if a is None or b is None:
        return False
    if isinstance(a, float) and isinstance(b, float):
        if a == b:
            return True
        if abs(a - b) <= abs_tolerance:
            return True
        if a != 0 and abs((a - b) / a) <= rel_tolerance:
            return True
        return False
    return a == b


def compare_results(
    genie_result: dict | None,
    expected_result: dict | None,
) -> ErrorAnalysisResult:
    """Programmatic comparison of two SQL execution results.

    Args:
        genie_result: Result from executing Genie's SQL (columns, data, row_count, error)
        expected_result: Result from executing expected SQL

    Returns:
        ErrorAnalysisResult with label, reason, and method="programmatic"
    """
    # Both missing or errored
    if not genie_result and not expected_result:
        return ErrorAnalysisResult(
            label="inconclusive",
            reason="Both results are missing",
            method="programmatic",
        )

    genie_error = genie_result.get("error") if genie_result else "No result"
    expected_error = expected_result.get("error") if expected_result else "No result"

    if genie_error and expected_error:
        return ErrorAnalysisResult(
            label="inconclusive",
            reason=f"Both queries errored. Genie: {genie_error}; Expected: {expected_error}",
            method="programmatic",
        )

    if genie_error:
        return ErrorAnalysisResult(
            label="incorrect",
            reason=f"Genie query errored: {genie_error}",
            method="programmatic",
        )

    if expected_error:
        return ErrorAnalysisResult(
            label="inconclusive",
            reason=f"Expected query errored: {expected_error}",
            method="programmatic",
        )

    if not genie_result or not expected_result:
        return ErrorAnalysisResult(
            label="inconclusive",
            reason="One or both results are missing",
            method="programmatic",
        )

    genie_cols = genie_result.get("columns", [])
    expected_cols = expected_result.get("columns", [])
    genie_data = genie_result.get("data", [])
    expected_data = expected_result.get("data", [])

    # Normalize column names
    genie_col_names = [_normalize_column_name(c.get("name", "")) for c in genie_cols]
    expected_col_names = [_normalize_column_name(c.get("name", "")) for c in expected_cols]

    # Check column count
    if len(genie_col_names) != len(expected_col_names):
        return ErrorAnalysisResult(
            label="incorrect",
            reason=f"Column count mismatch: Genie has {len(genie_col_names)} columns, expected {len(expected_col_names)}",
            method="programmatic",
        )

    # Check column names match (order-insensitive)
    if set(genie_col_names) != set(expected_col_names):
        return ErrorAnalysisResult(
            label="incorrect",
            reason=f"Column name mismatch: Genie has {sorted(genie_col_names)}, expected {sorted(expected_col_names)}",
            method="programmatic",
        )

    # Build column mapping (reorder expected data to match genie column order)
    col_mapping = []
    for g_name in genie_col_names:
        for idx, e_name in enumerate(expected_col_names):
            if g_name == e_name:
                col_mapping.append(idx)
                break

    # Check row count
    genie_row_count = len(genie_data)
    expected_row_count = len(expected_data)

    if genie_row_count == 0 and expected_row_count == 0:
        return ErrorAnalysisResult(
            label="correct",
            reason="Both queries returned empty results with matching columns",
            method="programmatic",
        )

    # Large row count difference -> incorrect
    if genie_row_count != expected_row_count:
        ratio = max(genie_row_count, expected_row_count) / max(min(genie_row_count, expected_row_count), 1)
        if ratio > 2 or abs(genie_row_count - expected_row_count) > 10:
            return ErrorAnalysisResult(
                label="incorrect",
                reason=f"Row count mismatch: Genie returned {genie_row_count} rows, expected {expected_row_count}",
                method="programmatic",
            )
        # Close but not identical -> uncertain, fall through to data comparison

    # Normalize and sort data for comparison
    def normalize_row(row, mapping=None):
        if mapping:
            row = [row[i] if i < len(row) else None for i in mapping]
        return tuple(_normalize_value(v) for v in row)

    genie_normalized = sorted(normalize_row(r) for r in genie_data)
    expected_normalized = sorted(normalize_row(r, col_mapping) for r in expected_data)

    # If different row counts but close, check data overlap
    if len(genie_normalized) != len(expected_normalized):
        return ErrorAnalysisResult(
            label="uncertain",
            reason=f"Row count differs slightly ({genie_row_count} vs {expected_row_count}), needs LLM review",
            method="programmatic",
        )

    # Compare cell-by-cell
    mismatches = 0
    total_cells = len(genie_normalized) * (len(genie_col_names) or 1)

    for g_row, e_row in zip(genie_normalized, expected_normalized):
        for g_val, e_val in zip(g_row, e_row):
            if not _values_match(g_val, e_val):
                mismatches += 1

    if mismatches == 0:
        return ErrorAnalysisResult(
            label="correct",
            reason=f"Results match: {genie_row_count} rows, {len(genie_col_names)} columns",
            method="programmatic",
        )

    mismatch_pct = (mismatches / total_cells * 100) if total_cells > 0 else 100

    if mismatch_pct > 20:
        return ErrorAnalysisResult(
            label="incorrect",
            reason=f"Data mismatch: {mismatches} of {total_cells} cells differ ({mismatch_pct:.0f}%)",
            method="programmatic",
        )

    # Ambiguous - small number of mismatches
    return ErrorAnalysisResult(
        label="uncertain",
        reason=f"Partial data mismatch: {mismatches} of {total_cells} cells differ ({mismatch_pct:.1f}%), needs LLM review",
        method="programmatic",
    )


def llm_compare_results(
    question_text: str,
    genie_sql: str | None,
    expected_sql: str | None,
    genie_result: dict | None,
    expected_result: dict | None,
    programmatic_reason: str,
) -> ErrorAnalysisResult:
    """Use LLM to determine if results are semantically equivalent.

    Called as fallback when programmatic comparison returns "uncertain".
    """
    import json
    from agent_server.prompts import get_error_analysis_prompt
    from agent_server.agent import get_analyzer

    prompt = get_error_analysis_prompt(
        question_text=question_text,
        genie_sql=genie_sql,
        expected_sql=expected_sql,
        genie_result=genie_result,
        expected_result=expected_result,
        programmatic_reason=programmatic_reason,
    )

    try:
        analyzer = get_analyzer()
        response_text = analyzer.call_llm(prompt)

        # Parse JSON response
        # Find JSON in response
        start = response_text.find('{')
        end = response_text.rfind('}') + 1
        if start >= 0 and end > start:
            result = json.loads(response_text[start:end])
            is_correct = result.get("is_correct", False)
            reason = result.get("reason", "LLM analysis")
            return ErrorAnalysisResult(
                label="correct" if is_correct else "incorrect",
                reason=reason,
                method="llm",
            )
    except Exception as e:
        logger.exception(f"LLM comparison failed: {e}")

    # If LLM fails, return inconclusive
    return ErrorAnalysisResult(
        label="inconclusive",
        reason=f"LLM analysis failed. Programmatic analysis: {programmatic_reason}",
        method="programmatic",
    )


def auto_label_items(items: list[dict]) -> list[dict]:
    """Auto-label a batch of items using hybrid analysis.

    Args:
        items: List of dicts with question_id, question_text, genie_sql, expected_sql,
               genie_result, expected_result

    Returns:
        List of dicts with question_id, auto_label, reason, method
    """
    results = []

    for item in items:
        # First try programmatic comparison
        result = compare_results(
            genie_result=item.get("genie_result"),
            expected_result=item.get("expected_result"),
        )

        # If uncertain, try LLM fallback
        if result.label == "uncertain":
            result = llm_compare_results(
                question_text=item.get("question_text", ""),
                genie_sql=item.get("genie_sql"),
                expected_sql=item.get("expected_sql"),
                genie_result=item.get("genie_result"),
                expected_result=item.get("expected_result"),
                programmatic_reason=result.reason,
            )

        results.append({
            "question_id": item["question_id"],
            "auto_label": result.label,
            "reason": result.reason,
            "method": result.method,
        })

    return results
