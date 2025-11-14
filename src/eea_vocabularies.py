#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
eea_vocabularies.py
Description: Module for loading downloaded EEA vocabularies
Author: Giovanni Bonafè | ARPA-FVG
Created: 2025-11-04
"""

from pathlib import Path
import json
from typing import Dict, Optional

CACHE_DIR = Path("eea_vocabularies")

class EEAVocabularies:
    """Class to manage EEA vocabularies from downloaded files"""
    
    def __init__(self, auto_load: bool = True):
        self.raw_data = {}
        self.vocabularies = {}
        if auto_load:
            self.load_common_vocabularies()
    
    def load_common_vocabularies(self):
        """Load most commonly used vocabularies"""
        common_vocabs = ['pollutant', 'quality_flag', 'unit', 'aggregation_process']
        for vocab_type in common_vocabs:
            self.load_vocabulary(vocab_type)
    
    def load_vocabulary(self, vocabulary_type: str) -> bool:
        """Load specific vocabulary from cached file. Returns True if successful."""
        cache_file = CACHE_DIR / f"{vocabulary_type}.json"
        
        if not cache_file.exists():
            print(f"Vocabulary file not found: {cache_file}")
            return False
        
        try:
            with open(cache_file, 'r', encoding='utf-8') as f:
                raw_data = json.load(f)
                self.raw_data[vocabulary_type] = raw_data
                
                # Extract simple code->name mapping if possible
                vocabulary = self._extract_vocabulary_mapping(raw_data, vocabulary_type)
                if vocabulary:
                    self.vocabularies[vocabulary_type] = vocabulary
                
            print(f"✓ Loaded {vocabulary_type} vocabulary")
            return True
            
        except Exception as e:
            print(f"Error loading {vocabulary_type}: {e}")
            return False
    
    def _extract_vocabulary_mapping(self, raw_data: dict, vocabulary_type: str) -> Optional[Dict[str, str]]:
        """Try to extract code->name mapping from raw JSON data"""
        try:
            vocabulary = {}
            
            # Common EIONET JSON structure
            if 'results' in raw_data and isinstance(raw_data['results'], list):
                for item in raw_data['results']:
                    if 'notation' in item and 'prefLabel' in item:
                        code = item['notation']
                        label = item['prefLabel']
                        
                        if isinstance(label, dict):
                            label = label.get('en', next(iter(label.values()), ''))
                        elif not isinstance(label, str):
                            continue
                        
                        vocabulary[code] = label
            
            return vocabulary if vocabulary else None
            
        except Exception:
            return None
    
    def get_raw_data(self, vocabulary_type: str) -> Optional[dict]:
        """Get raw JSON data for vocabulary"""
        return self.raw_data.get(vocabulary_type)
    
    def get_name(self, vocabulary_type: str, code: str) -> Optional[str]:
        """Get name for a code in specified vocabulary. Returns None if not available."""
        vocab = self.vocabularies.get(vocabulary_type)
        if vocab is None:
            return None
        return vocab.get(str(code))
    
    # Convenience methods for common vocabularies
    def get_pollutant_name(self, code: str) -> Optional[str]:
        return self.get_name('pollutant', code)
    
    def get_quality_flag(self, code: str) -> Optional[str]:
        return self.get_name('quality_flag', code)
    
    def get_unit_name(self, code: str) -> Optional[str]:
        return self.get_name('unit', code)
    
    def get_aggregation_process(self, code: str) -> Optional[str]:
        return self.get_name('aggregation_process', code)
    
    def get_station_type(self, code: str) -> Optional[str]:
        return self.get_name('station_type', code)
    
    def get_measurement_method(self, code: str) -> Optional[str]:
        return self.get_name('measurement_method', code)
    
    def get_sampling_method(self, code: str) -> Optional[str]:
        return self.get_name('sampling_method', code)
    
    def is_vocabulary_loaded(self, vocabulary_type: str) -> bool:
        """Check if a vocabulary is loaded"""
        return vocabulary_type in self.raw_data

    def get_loaded_vocabularies(self) -> list:
        """Get list of loaded vocabulary types"""
        return list(self.raw_data.keys())

# Global instance for easy access
vocab_manager = EEAVocabularies()



