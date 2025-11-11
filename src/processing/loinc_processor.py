"""
LOINC Processing Strategy:
- One LOINC code = one chunk
- Include: code, long name, short name, component, system, property
- Add clinical context where available
"""

import csv
import zipfile
from pathlib import Path
from typing import List, Dict
import json
from datetime import datetime

import sys
sys.path.append(str(Path(__file__).parent.parent))

from models.chunk import Chunk


class LOINCProcessor:
    """Process LOINC Radiology Playbook into structured chunks"""
    
    def __init__(self, loinc_dir: Path):
        self.loinc_dir = loinc_dir
        self.chunks: List[Chunk] = []
        
        # Find the Radiology Playbook CSV
        self.playbook_file = None
        extracted_dir = loinc_dir / "extracted"
        
        if extracted_dir.exists():
            # Search for RadiologyPlaybook CSV
            for csv_file in extracted_dir.rglob("*RadiologyPlaybook*.csv"):
                self.playbook_file = csv_file
                break
        
        if not self.playbook_file:
            # Try to extract from zip
            zip_files = list(loinc_dir.glob("Loinc_*.zip"))
            if zip_files:
                self.zip_file = zip_files[0]
            else:
                self.zip_file = None
    
    def load_loinc(self) -> List[Dict]:
        """
        Load LOINC data from Radiology Playbook CSV.
        
        Main file is LoincRsnaRadiologyPlaybook.csv which contains:
        - LOINC codes
        - Long common names
        - Anatomical parts
        - RadLex mappings
        """
        print(f"   Loading LOINC Radiology Playbook...")
        
        procedures = []
        
        if self.playbook_file and self.playbook_file.exists():
            # Load from extracted file
            print(f"   Loading from: {self.playbook_file}")
            with open(self.playbook_file, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                procedures = list(reader)
        
        elif self.zip_file and self.zip_file.exists():
            # Extract and load from zip
            print(f"   Extracting from: {self.zip_file}")
            with zipfile.ZipFile(self.zip_file, 'r') as zf:
                playbook_files = [f for f in zf.namelist() 
                                if 'RadiologyPlaybook' in f and f.endswith('.csv')]
                
                if not playbook_files:
                    raise FileNotFoundError("RadiologyPlaybook.csv not found in zip")
                
                csv_name = playbook_files[0]
                print(f"   Found: {Path(csv_name).name}")
                
                # Read CSV from zip
                with zf.open(csv_name) as csv_file:
                    text_file = (line.decode('utf-8') for line in csv_file)
                    reader = csv.DictReader(text_file)
                    procedures = list(reader)
        
        else:
            raise FileNotFoundError(f"No LOINC data found in {self.loinc_dir}")
        
        print(f"   Loaded {len(procedures):,} LOINC procedure records")
        
        return procedures
    
    def process(self) -> List[Chunk]:
        """
        Process LOINC codes into chunks.
        
        Each chunk contains:
        - LOINC code
        - Long common name
        - Short name
        - Component
        - Property
        - System (specimen type)
        - Anatomical parts
        - RadLex ID mappings
        """
        print("\n   Processing LOINC procedures into chunks...")
        
        # Load procedures
        procedures = self.load_loinc()
        
        # Group by LOINC number to aggregate parts
        from collections import defaultdict
        loinc_groups = defaultdict(list)
        
        for proc in procedures:
            loinc_num = proc.get('LoincNumber', '')
            if loinc_num:
                loinc_groups[loinc_num].append(proc)
        
        print(f"   Grouped into {len(loinc_groups):,} unique LOINC codes")
        
        # Create chunk for each unique LOINC code
        for loinc_num, parts in loinc_groups.items():
            # Use first part for main info
            main = parts[0]
            
            # Build rich text description
            text_parts = []
            
            # LOINC code and name
            text_parts.append(f"LOINC: {loinc_num}")
            
            if main.get('LongCommonName'):
                text_parts.append(f"Name: {main['LongCommonName']}")
            
            if main.get('ShortName'):
                text_parts.append(f"Short Name: {main['ShortName']}")
            
            # Aggregate anatomical parts
            anatomies = set()
            radlex_ids = set()
            part_types = set()
            
            for part in parts:
                if part.get('PartName'):
                    anatomies.add(part['PartName'])
                if part.get('RID'):
                    radlex_ids.add(part['RID'])
                if part.get('PartTypeName'):
                    part_types.add(part['PartTypeName'])
            
            # Add anatomical info
            if anatomies:
                text_parts.append(f"Component: {', '.join(sorted(anatomies))}")
            
            # Add RadLex cross-references
            if radlex_ids:
                text_parts.append(f"RadLex IDs: {', '.join(sorted(radlex_ids))}")
            
            # Add component types
            if part_types:
                clean_types = [pt.replace('Rad.', '').replace('.', ' ').strip() for pt in part_types]
                text_parts.append(f"System: {', '.join(sorted(clean_types))}")
            
            # Additional fields if available
            if main.get('Property'):
                text_parts.append(f"Property: {main['Property']}")
            
            chunk_text = '\n'.join(text_parts)
            
            # Create chunk with metadata
            chunk = Chunk(
                chunk_id=f"loinc_{loinc_num.replace('-', '_')}",
                text=chunk_text,
                source_type="loinc",
                metadata={
                    "source_file": "LoincRsnaRadiologyPlaybook.csv",
                    "created_at": datetime.now().isoformat(),
                    "code": loinc_num,
                    "procedure_name": main.get('LongCommonName', ''),
                    "anatomical_parts": list(anatomies),
                    "radlex_ids": list(radlex_ids),
                    "part_count": len(parts),
                    "category": "lab_terminology",
                    "char_count": len(chunk_text)
                }
            )
            
            self.chunks.append(chunk)
        
        print(f"   ✓ Created {len(self.chunks):,} chunks")
        
        return self.chunks
    
    def save_chunks(self, output_path: Path):
        """Save chunks to JSONL"""
        print(f"   Saving to: {output_path}")
        
        with open(output_path, 'w', encoding='utf-8') as f:
            for chunk in self.chunks:
                f.write(json.dumps(chunk.to_dict()) + '\n')
        
        print(f"   ✓ Saved {len(self.chunks):,} chunks")
