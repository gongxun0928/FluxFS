#!/usr/bin/env python3
"""Run pinned pjdfstest cases one-by-one and emit JSON + JUnit reports."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import pathlib
import re
import subprocess
import time
import xml.etree.ElementTree as ET


VALID_REASONS = {"deferred", "env-limit", "bug"}
BLOCKING_DISPOSITIONS = {
    "unexpected-fail",
    "unexpected-pass",
    "expected-failure-mismatch",
    "known-bug-fail",
}


def load_json(path: pathlib.Path) -> dict:
    with path.open(encoding="utf-8") as stream:
        return json.load(stream)


def sha256_file(path: pathlib.Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be greater than zero")
    return parsed


def validate_configuration(suite: dict, known_document: dict) -> dict[str, dict]:
    if suite.get("mode") != known_document.get("mode"):
        raise SystemExit("suite and known-fail mode mismatch")

    cases = suite.get("cases")
    entries = known_document.get("known_failures")
    if not isinstance(cases, list) or not isinstance(entries, list):
        raise SystemExit("suite cases and known_failures must be arrays")

    case_by_test: dict[str, dict] = {}
    for case in cases:
        test = case.get("test")
        category = case.get("category")
        path = pathlib.PurePosixPath(test) if isinstance(test, str) else None
        if (
            path is None
            or not test.endswith(".t")
            or path.is_absolute()
            or ".." in path.parts
        ):
            raise SystemExit(f"invalid suite test path: {case}")
        if not isinstance(category, str) or not category:
            raise SystemExit(f"invalid suite category: {case}")
        if test in case_by_test:
            raise SystemExit(f"duplicate suite test: {test}")
        case_by_test[test] = case

    known_by_test: dict[str, dict] = {}
    for entry in entries:
        test = entry.get("test")
        if test == "*":
            raise SystemExit("wildcard known-fail entries are forbidden")
        if test not in case_by_test:
            raise SystemExit(f"known-fail entry is not in suite: {entry}")
        if test in known_by_test:
            raise SystemExit(f"duplicate known-fail entry: {test}")
        if not isinstance(entry.get("category"), str) or not entry["category"]:
            raise SystemExit(f"invalid known-fail category: {entry}")
        if entry.get("reason") not in VALID_REASONS:
            raise SystemExit(f"invalid known-fail reason: {entry}")
        if not isinstance(entry.get("detail"), str) or not entry["detail"].strip():
            raise SystemExit(f"known-fail detail is required: {entry}")
        if not isinstance(entry.get("expected_output"), str) or not entry[
            "expected_output"
        ]:
            raise SystemExit(f"known-fail expected_output is required: {entry}")
        try:
            re.compile(entry["expected_output"])
        except re.error as error:
            raise SystemExit(f"invalid known-fail expected_output: {entry}: {error}")
        known_by_test[test] = entry
    return known_by_test


def classify_result(passed: bool, known: dict | None, output: str = "") -> str:
    if passed and known is None:
        return "pass"
    if not passed and known is None:
        return "unexpected-fail"
    if passed:
        return "unexpected-pass"
    if re.search(known["expected_output"], output, flags=re.MULTILINE) is None:
        return "expected-failure-mismatch"
    if known["reason"] == "bug":
        return "known-bug-fail"
    return "expected-fail"


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--suite", required=True, type=pathlib.Path)
    parser.add_argument("--known-fail", required=True, type=pathlib.Path)
    parser.add_argument("--pjdfstest-dir", required=True, type=pathlib.Path)
    parser.add_argument("--mountpoint", required=True, type=pathlib.Path)
    parser.add_argument("--report-dir", required=True, type=pathlib.Path)
    parser.add_argument("--revision", required=True)
    parser.add_argument("--fluxfs-revision", required=True)
    parser.add_argument("--fluxfs-dirty", required=True, choices=("true", "false"))
    parser.add_argument("--timeout-seconds", type=positive_int, default=60)
    args = parser.parse_args()

    suite = load_json(args.suite)
    known_document = load_json(args.known_fail)
    known_by_test = validate_configuration(suite, known_document)

    args.report_dir.mkdir(parents=True, exist_ok=True)
    raw_dir = args.report_dir / f"pjdfstest-{suite['mode']}-tap"
    raw_dir.mkdir(parents=True, exist_ok=True)
    results: list[dict] = []

    for case in suite["cases"]:
        test = case["test"]
        test_path = args.pjdfstest_dir / "tests" / test
        if not test_path.is_file():
            raise SystemExit(f"pinned pjdfstest case does not exist: {test}")
        started = time.monotonic()
        try:
            completed = subprocess.run(
                ["prove", "-v", str(test_path)],
                cwd=args.mountpoint,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                env={**os.environ, "LC_ALL": "C"},
                check=False,
                timeout=args.timeout_seconds,
            )
            output = completed.stdout
            exit_code = completed.returncode
        except subprocess.TimeoutExpired as error:
            partial = error.stdout or ""
            if isinstance(partial, bytes):
                partial = partial.decode(errors="replace")
            output = f"{partial}\nrunner timeout after {args.timeout_seconds}s\n"
            exit_code = 124
        duration = time.monotonic() - started
        raw_name = test.replace("/", "_") + ".tap"
        (raw_dir / raw_name).write_text(output, encoding="utf-8")
        known = known_by_test.get(test)
        passed = exit_code == 0
        disposition = classify_result(passed, known, output)
        results.append(
            {
                "test": test,
                "category": case["category"],
                "passed": passed,
                "exit_code": exit_code,
                "duration_seconds": round(duration, 6),
                "disposition": disposition,
                "known_failure": known,
                "tap": str(raw_dir / raw_name),
            }
        )
        print(f"{disposition:16} {test}", flush=True)

    counts = {
        disposition: sum(result["disposition"] == disposition for result in results)
        for disposition in sorted({result["disposition"] for result in results})
    }
    report = {
        "schema_version": 1,
        "mode": suite["mode"],
        "pjdfstest_revision": args.revision,
        "fluxfs_revision": args.fluxfs_revision,
        "fluxfs_dirty": args.fluxfs_dirty == "true",
        "timeout_seconds": args.timeout_seconds,
        "suite_sha256": sha256_file(args.suite),
        "known_fail_sha256": sha256_file(args.known_fail),
        "counts": counts,
        "results": results,
    }
    stem = args.report_dir / f"pjdfstest-{suite['mode']}"
    stem.with_suffix(".json").write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )

    testsuite = ET.Element(
        "testsuite",
        name=f"pjdfstest-{suite['mode']}",
        tests=str(len(results)),
        failures=str(
            sum(
                result["disposition"] in BLOCKING_DISPOSITIONS
                for result in results
            )
        ),
        skipped=str(sum(result["disposition"] == "expected-fail" for result in results)),
        time=f"{sum(result['duration_seconds'] for result in results):.6f}",
    )
    for result in results:
        testcase = ET.SubElement(
            testsuite,
            "testcase",
            classname=f"pjdfstest.{suite['mode']}.{result['category']}",
            name=result["test"],
            time=f"{result['duration_seconds']:.6f}",
        )
        disposition = result["disposition"]
        if disposition == "expected-fail":
            known = result["known_failure"]
            ET.SubElement(
                testcase,
                "skipped",
                message=f"{known['reason']}: {known['detail']}",
            )
        elif disposition != "pass":
            ET.SubElement(testcase, "failure", message=disposition).text = pathlib.Path(
                result["tap"]
            ).read_text(encoding="utf-8")
    ET.ElementTree(testsuite).write(stem.with_suffix(".xml"), encoding="utf-8", xml_declaration=True)

    return (
        1
        if any(
            result["disposition"] in BLOCKING_DISPOSITIONS for result in results
        )
        else 0
    )


if __name__ == "__main__":
    raise SystemExit(main())
