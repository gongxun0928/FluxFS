#!/usr/bin/env python3
"""Unit tests for pjdfstest gate configuration and anti-fake-green rules."""

import argparse
import unittest

import report


class ReportTest(unittest.TestCase):
    def test_timeout_must_be_positive(self) -> None:
        self.assertEqual(report.positive_int("60"), 60)
        with self.assertRaisesRegex(argparse.ArgumentTypeError, "greater than zero"):
            report.positive_int("0")

    def test_dispositions_are_fail_closed(self) -> None:
        deferred = {
            "test": "open/01.t",
            "category": "scope",
            "reason": "deferred",
            "detail": "not in the current contract",
            "expected_output": "got ENOSYS$",
            "expected_not_ok": 1,
        }
        bug = {**deferred, "reason": "bug"}
        self.assertEqual(report.classify_result(True, None), "pass")
        self.assertEqual(report.classify_result(False, None), "unexpected-fail")
        self.assertEqual(report.classify_result(True, deferred), "unexpected-pass")
        self.assertEqual(
            report.classify_result(False, deferred, "not ok 1: got ENOSYS"),
            "expected-fail",
        )
        self.assertEqual(
            report.classify_result(False, deferred, "not ok 1: got EIO"),
            "expected-failure-mismatch",
        )
        self.assertEqual(
            report.classify_result(False, deferred, "not ok 1: got ENOSYS\nnot ok 2: got EIO"),
            "expected-failure-mismatch",
        )
        self.assertEqual(
            report.classify_result(False, bug, "not ok 1: got ENOSYS"),
            "known-bug-fail",
        )

    def test_empty_suite_is_rejected(self) -> None:
        with self.assertRaisesRegex(SystemExit, "must not be empty"):
            report.validate_configuration(
                {"mode": "ephemeral", "cases": []},
                {"mode": "ephemeral", "known_failures": []},
            )

    def test_wildcard_known_failure_is_rejected(self) -> None:
        suite = {
            "mode": "ephemeral",
            "cases": [{"test": "open/01.t", "category": "open"}],
        }
        known = {
            "mode": "ephemeral",
            "known_failures": [
                {
                    "test": "*",
                    "category": "scope",
                    "reason": "deferred",
                    "detail": "too broad",
                    "expected_output": "got ENOSYS",
                    "expected_not_ok": 1,
                }
            ],
        }
        with self.assertRaisesRegex(SystemExit, "wildcard"):
            report.validate_configuration(suite, known)

    def test_orphan_known_failure_is_rejected(self) -> None:
        suite = {
            "mode": "ephemeral",
            "cases": [{"test": "open/01.t", "category": "open"}],
        }
        known = {
            "mode": "ephemeral",
            "known_failures": [
                {
                    "test": "open/02.t",
                    "category": "scope",
                    "reason": "deferred",
                    "detail": "not in suite",
                    "expected_output": "got ENOSYS",
                    "expected_not_ok": 1,
                }
            ],
        }
        with self.assertRaisesRegex(SystemExit, "not in suite"):
            report.validate_configuration(suite, known)

    def test_invalid_suite_and_known_failure_fields_are_rejected(self) -> None:
        good_case = {"test": "open/01.t", "category": "open"}
        good_known = {
            "test": "open/01.t",
            "category": "scope",
            "reason": "deferred",
            "detail": "unsupported matrix",
            "expected_output": "got ENOSYS$",
            "expected_not_ok": 1,
        }
        invalid = [
            ([good_case, good_case], [good_known], "duplicate suite"),
            ([{"test": "../open/01.t", "category": "open"}], [], "invalid suite"),
            ([{"test": "open/01", "category": "open"}], [], "invalid suite"),
            ([good_case], [good_known, good_known], "duplicate known-fail"),
            ([good_case], [{**good_known, "reason": "ignore"}], "invalid known-fail reason"),
            (
                [good_case],
                [{**good_known, "expected_output": "["}],
                "invalid known-fail expected_output",
            ),
            (
                [good_case],
                [{**good_known, "expected_not_ok": 0}],
                "expected_not_ok must be positive",
            ),
        ]
        for cases, known_failures, message in invalid:
            with self.subTest(message=message):
                with self.assertRaisesRegex(SystemExit, message):
                    report.validate_configuration(
                        {"mode": "ephemeral", "cases": cases},
                        {
                            "mode": "ephemeral",
                            "known_failures": known_failures,
                        },
                    )


if __name__ == "__main__":
    unittest.main()
