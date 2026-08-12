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
        }
        bug = {**deferred, "reason": "bug"}
        self.assertEqual(report.classify_result(True, None), "pass")
        self.assertEqual(report.classify_result(False, None), "unexpected-fail")
        self.assertEqual(report.classify_result(True, deferred), "unexpected-pass")
        self.assertEqual(
            report.classify_result(False, deferred, "not ok: got ENOSYS"),
            "expected-fail",
        )
        self.assertEqual(
            report.classify_result(False, deferred, "not ok: got EIO"),
            "expected-failure-mismatch",
        )
        self.assertEqual(
            report.classify_result(False, bug, "not ok: got ENOSYS"),
            "known-bug-fail",
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
                }
            ],
        }
        with self.assertRaisesRegex(SystemExit, "not in suite"):
            report.validate_configuration(suite, known)


if __name__ == "__main__":
    unittest.main()
