#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
eea_vocabularies.py
Description: Module for loading downloaded EEA vocabularies
Author: Giovanni Bonafè | ARPA-FVG
Created: 2025-11-04
"""

from __future__ import annotations

import json
import logging
import math
import re
from pathlib import Path
from typing import Dict, Optional, Tuple

logger = logging.getLogger(__name__)

DEFAULT_CACHE_DIR = Path("eea_vocabularies")
_STATION_ID_RE = re.compile(r"([A-Z]{2}\d+[A-Z]?)")


def _normalize_label(value) -> Optional[str]:
    """Best-effort conversion of label payloads to simple english strings."""
    if value is None:
        return None
    if isinstance(value, str):
        text = value.strip()
        return text or None

    if isinstance(value, dict):
        # Common shapes: {"en": "Nitrogen dioxide"} or {"@value": "..."}
        candidates = (
            value.get("en"),
            value.get("@value"),
            value.get("value"),
            next(iter(value.values()), None) if value else None,
        )
        for candidate in candidates:
            normalized = _normalize_label(candidate)
            if normalized:
                return normalized
        return None

    if isinstance(value, list):
        for item in value:
            normalized = _normalize_label(item)
            if normalized:
                return normalized
        return None

    return str(value).strip() or None


class EEAVocabularies:
    """Class to manage EEA vocabularies from downloaded files."""

    def __init__(
        self,
        cache_dir: Path | str = DEFAULT_CACHE_DIR,
        auto_load: bool = True,
        default_vocabularies: Optional[Tuple[str, ...]] = None,
    ) -> None:
        self.cache_dir = Path(cache_dir)
        self.raw_data: Dict[str, dict] = {}
        self.vocabularies: Dict[str, Dict[str, Dict[str, str]]] = {}
        if auto_load:
            self.load_common_vocabularies(default_vocabularies)

    def load_common_vocabularies(self, vocabulary_names: Optional[Tuple[str, ...]] = None) -> None:
        common_vocabs = vocabulary_names or (
            "pollutant",
            "quality_flag",
            "unit",
            "aggregation_process",
        )
        for vocab_type in common_vocabs:
            self.load_vocabulary(vocab_type)

    def load_vocabulary(self, vocabulary_type: str) -> bool:
        """Load specific vocabulary from cached file. Returns True if successful."""
        cache_file = self.cache_dir / f"{vocabulary_type}.json"

        if not cache_file.exists():
            logger.warning("Vocabulary file not found: %s", cache_file)
            return False

        try:
            with open(cache_file, "r", encoding="utf-8") as f:
                raw_data = json.load(f)
                self.raw_data[vocabulary_type] = raw_data

                labels, notation = self._extract_vocabulary_mapping(raw_data, vocabulary_type)
                if labels:
                    self.vocabularies[vocabulary_type] = {
                        "labels": labels,
                        "notation": notation,
                    }
                    logger.info(
                        "✓ Loaded %s vocabulary (%d labels)",
                        vocabulary_type,
                        len(labels),
                    )
                else:
                    logger.warning(
                        "Vocabulary %s did not yield any label mappings", vocabulary_type
                    )
            return True

        except Exception as exc:  # pylint: disable=broad-except
            logger.error("Error loading %s: %s", vocabulary_type, exc)
            return False

    def _extract_vocabulary_mapping(
        self, raw_data: dict, vocabulary_type: str
    ) -> Tuple[Dict[str, str], Dict[str, str]]:
        """Extract code->name and code->notation mappings from raw JSON data."""
        records = []
        if isinstance(raw_data, dict):
            for candidate_key in ("results", "concepts", "items"):
                value = raw_data.get(candidate_key)
                if isinstance(value, list) and value and isinstance(value[0], dict):
                    records = value
                    break
            if not records:
                # Sometimes data is nested under arbitrary keys.
                for value in raw_data.values():
                    if isinstance(value, list) and value and isinstance(value[0], dict):
                        records = value
                        break
        elif isinstance(raw_data, list):
            records = raw_data

        label_map: Dict[str, str] = {}
        notation_map: Dict[str, str] = {}

        for item in records:
            if not isinstance(item, dict):
                continue
            code = (
                item.get("notation")
                or item.get("Notation")
                or item.get("id")
                or item.get("@id")
            )
            if code is None:
                continue
            code_str = str(code)

            label = (
                _normalize_label(item.get("prefLabel"))
                or _normalize_label(item.get("label"))
                or _normalize_label(item.get("name"))
            )
            if label:
                label_map[code_str] = label

            notation_value = item.get("Notation") or item.get("notation")
            if notation_value:
                notation_map[code_str] = str(notation_value)

        if not label_map:
            logger.debug("No mappings extracted for vocabulary %s", vocabulary_type)

        return label_map, notation_map

    def get_label_mapping(self, vocabulary_type: str) -> Dict[str, str]:
        return self.vocabularies.get(vocabulary_type, {}).get("labels", {})

    def get_notation_mapping(self, vocabulary_type: str) -> Dict[str, str]:
        return self.vocabularies.get(vocabulary_type, {}).get("notation", {})

    def get_raw_data(self, vocabulary_type: str) -> Optional[dict]:
        """Get raw JSON data for vocabulary."""
        return self.raw_data.get(vocabulary_type)

    def get_name(self, vocabulary_type: str, code: str) -> Optional[str]:
        """Get name for a code in specified vocabulary. Returns None if not available."""
        return self.get_label_mapping(vocabulary_type).get(str(code))

    def is_vocabulary_loaded(self, vocabulary_type: str) -> bool:
        """Check if a vocabulary is loaded."""
        return vocabulary_type in self.raw_data

    def get_loaded_vocabularies(self) -> list[str]:
        """Get list of loaded vocabulary types."""
        return list(self.raw_data.keys())

    def summarize(self) -> Dict[str, int]:
        """Return counts for each loaded vocabulary."""
        return {
            vocab_type: len(payload.get("labels", {}))
            for vocab_type, payload in self.vocabularies.items()
        }


def clean_samplingpoint_id(value) -> Optional[str]:
    """Normalize Samplingpoint/Sampling Point Id strings used across scripts."""
    if value is None:
        return None
    if isinstance(value, float) and math.isnan(value):
        return None

    text = str(value).strip()
    if not text:
        return None

    # Remove country prefixes such as IT/SPO.
    if "/" in text:
        text = text.split("/")[-1]

    # Keep the portion before the first underscore when available.
    core = text.split("_")[0]
    match = _STATION_ID_RE.search(core)
    if match:
        return match.group(1)
    return core or None


__all__ = ["EEAVocabularies", "clean_samplingpoint_id"]
