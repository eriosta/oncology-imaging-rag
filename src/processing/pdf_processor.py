"""
PDF Processing Strategy:
For TNM 9th Edition Lung Cancer Protocol PDF:
1. Extract text with structure preservation
2. Apply semantic chunking by sections (T/N/M classifications, staging rules)
3. Preserve lung cancer staging criteria and category metadata
4. Create contextually-rich chunks for RAG retrieval

For RECIST PDF:
1. Semantic chunking by section
2. Extract measurement criteria carefully
3. Preserve context about response evaluation
"""

try:
    import fitz  # PyMuPDF
    PYMUPDF_AVAILABLE = True
except ImportError:
    PYMUPDF_AVAILABLE = False

from pathlib import Path
from typing import List, Dict
import json
from datetime import datetime
import re

import sys
sys.path.append(str(Path(__file__).parent.parent))

from models.chunk import Chunk


class TNMProcessor:
    """
    Process TNM 9th Edition Lung Cancer Protocol PDF into structured chunks.
    
    Extracts lung cancer staging criteria including T/N/M classifications,
    staging rules, and clinical criteria for accurate cancer staging.
    """
    
    def __init__(self, pdf_path: Path):
        self.pdf_path = pdf_path
        self.chunks: List[Chunk] = []
    
    def extract_text_with_structure(self) -> List[Dict]:
        """Extract text from PDF preserving page structure"""
        if not PYMUPDF_AVAILABLE:
            raise ImportError("PyMuPDF required. Install: pip install pymupdf")
        
        pages = []
        doc = fitz.open(self.pdf_path)
        
        for page_num, page in enumerate(doc, start=1):
            pages.append({
                'page_number': page_num,
                'text': page.get_text()
            })
        
        doc.close()
        return pages
    
    def detect_structure(self, pages: List[Dict]) -> List[Dict]:
        """
        Detect TNM document structure (cancer sites and staging components)
        
        Strategy: Identify key section headers but include ALL content
        """
        structured_elements = []
        
        for page in pages:
            page_num = page['page_number']
            text = page['text']
            lines = text.split('\n')
            
            current_section = None
            current_subsection = None
            buffer = []
            
            for i, line in enumerate(lines):
                line_stripped = line.strip()
                
                # Skip truly empty lines
                if not line_stripped:
                    if buffer and buffer[-1]:
                        buffer.append('')
                    continue
                
                # Detect major headers (cancer types, staging components)
                header_info = self._detect_tnm_header(line_stripped, lines[max(0, i-1):min(len(lines), i+3)])
                
                if header_info:
                    # Save previous buffer if exists
                    if buffer:
                        while buffer and not buffer[-1]:
                            buffer.pop()
                        
                        if buffer:
                            structured_elements.append({
                                'type': 'content',
                                'text': '\n'.join(buffer),
                                'page': page_num,
                                'section': current_section,
                                'subsection': current_subsection
                            })
                        buffer = []
                    
                    # Add header
                    structured_elements.append({
                        'type': 'header',
                        'level': header_info['level'],
                        'text': line_stripped,
                        'page': page_num
                    })
                    
                    # Update context
                    if header_info['level'] == 1:
                        current_section = line_stripped
                        current_subsection = None
                    elif header_info['level'] == 2:
                        current_subsection = line_stripped
                else:
                    buffer.append(line_stripped)
            
            # Save remaining buffer
            if buffer:
                while buffer and not buffer[-1]:
                    buffer.pop()
                
                if buffer:
                    structured_elements.append({
                        'type': 'content',
                        'text': '\n'.join(buffer),
                        'page': page_num,
                        'section': current_section,
                        'subsection': current_subsection
                    })
        
        return structured_elements
    
    def _detect_tnm_header(self, line: str, context: List[str]) -> Dict:
        """Detect TNM-specific headers (cancer types and staging components)"""
        
        # Skip organization headers
        if 'INTERNATIONAL ASSOCIATION' in line or line.startswith('E U R O P E A N'):
            return None
        
        # Cancer type + staging component
        tnm_pattern = re.match(
            r'^([A-Z][a-zA-Z\s]+(?:Cancer|Tumors?|Mesothelioma|Carcinoma))\s+(T|N|M|TNM)\s+(Classification|Definitions?|Stages?)',
            line, re.IGNORECASE
        )
        if tnm_pattern:
            return {'level': 1, 'pattern': 'tnm_cancer_type'}
        
        # Just cancer type section
        cancer_type_pattern = re.match(r'^([A-Z][a-zA-Z\s]+(?:Cancer|Tumors?|Mesothelioma|Carcinoma))', line)
        if cancer_type_pattern and ('–9th Edition' in line or 'Classification' in line or len(line.split()) <= 6):
            return {'level': 1, 'pattern': 'tnm_cancer_type'}
        
        # Numbered sections
        numbered_match = re.match(r'^(\d+\.)+\s+([A-Z][a-zA-Z].+)', line)
        if numbered_match:
            if not line.endswith('.') or len(line) < 80:
                level = min(numbered_match.group(1).count('.'), 3)
                return {'level': level, 'pattern': 'numbered'}
        
        # Appendix headers
        if re.match(r'^Appendix\s+[IVX]+', line, re.IGNORECASE):
            return {'level': 1, 'pattern': 'appendix'}
        
        # ALL CAPS multi-word headers
        if (line.isupper() and 
            2 <= len(line.split()) <= 8 and
            len(line) < 80 and
            not line.endswith('.')):
            return {'level': 1, 'pattern': 'all_caps'}
        
        return None
    
    def create_semantic_chunks(self, structured_elements: List[Dict]) -> List[Dict]:
        """Create chunks by semantic sections - group content under each header"""
        chunks = []
        chunk_id = 0
        
        i = 0
        while i < len(structured_elements):
            element = structured_elements[i]
            
            if element['type'] == 'header':
                section_header = element['text']
                section_level = element['level']
                section_page = element['page']
                
                # Collect all content until next header of same or higher level
                section_content = []
                section_subsection = None
                i += 1
                
                while i < len(structured_elements):
                    next_elem = structured_elements[i]
                    
                    # Stop at next header of same/higher level
                    if next_elem['type'] == 'header':
                        if next_elem['level'] <= section_level:
                            break
                        elif next_elem['level'] == section_level + 1:
                            section_subsection = next_elem['text']
                    
                    # Add content
                    if next_elem['type'] == 'content':
                        section_content.append(next_elem['text'])
                    
                    i += 1
                
                # Build complete section text with header
                if section_content:
                    full_section_text = f"{section_header}\n\n" + '\n\n'.join(section_content)
                    
                    # If section fits in one chunk, use it directly
                    if len(full_section_text) <= 3000:
                        chunks.append({
                            'id': f"chunk_{chunk_id}",
                            'text': full_section_text,
                            'page': section_page,
                            'section': section_header,
                            'subsection': section_subsection,
                            'char_count': len(full_section_text)
                        })
                        chunk_id += 1
                    else:
                        # Split at paragraph boundaries while keeping header context
                        paragraphs = full_section_text.split('\n\n')
                        current_chunk = [section_header]
                        current_length = len(section_header) + 2
                        
                        for para in paragraphs[1:]:
                            para_len = len(para)
                            
                            if current_length + para_len + 2 > 3000:
                                if len(current_chunk) > 1:
                                    chunks.append({
                                        'id': f"chunk_{chunk_id}",
                                        'text': '\n\n'.join(current_chunk),
                                        'page': section_page,
                                        'section': section_header,
                                        'subsection': section_subsection,
                                        'char_count': current_length
                                    })
                                    chunk_id += 1
                                
                                current_chunk = [section_header, para]
                                current_length = len(section_header) + para_len + 4
                            else:
                                current_chunk.append(para)
                                current_length += para_len + 2
                        
                        if len(current_chunk) > 1:
                            chunks.append({
                                'id': f"chunk_{chunk_id}",
                                'text': '\n\n'.join(current_chunk),
                                'page': section_page,
                                'section': section_header,
                                'subsection': section_subsection,
                                'char_count': current_length
                            })
                            chunk_id += 1
            else:
                # Content without header
                if element['type'] == 'content':
                    chunks.append({
                        'id': f"chunk_{chunk_id}",
                        'text': element['text'],
                        'page': element.get('page', 1),
                        'section': None,
                        'subsection': None,
                        'char_count': len(element['text'])
                    })
                    chunk_id += 1
                i += 1
        
        return chunks
    
    def _identify_cancer_type(self, text: str) -> str:
        """Extract cancer type from section text"""
        if not text:
            return "Unknown"
        
        cancer_keywords = [
            'lung', 'thyroid', 'breast', 'colon', 'prostate', 'liver', 'kidney',
            'bladder', 'stomach', 'esophagus', 'pancreas', 'melanoma', 'lymphoma',
            'leukemia', 'ovarian', 'cervical', 'testicular', 'brain', 'thymic'
        ]
        
        text_lower = text.lower()
        for keyword in cancer_keywords:
            if keyword in text_lower:
                return keyword.title()
        
        return "Unknown"
    
    def _identify_category(self, text: str) -> str:
        """Identify if this is T, N, M, or Stage grouping"""
        if not text:
            return "Unknown"
        
        text_upper = text.upper()
        
        if 'T CLASSIFICATION' in text_upper or 'T STAGE' in text_upper:
            return 'T-staging'
        elif 'N CLASSIFICATION' in text_upper or 'N STAGE' in text_upper:
            return 'N-staging'
        elif 'M CLASSIFICATION' in text_upper or 'M STAGE' in text_upper:
            return 'M-staging'
        elif 'TNM' in text_upper or 'STAGE GROUP' in text_upper:
            return 'TNM-staging'
        
        return 'Unknown'
    
    def process(self) -> List[Chunk]:
        """Process TNM Lung Cancer Protocol PDF into chunks"""
        print("\n   Processing TNM 9th Edition Lung Cancer Protocol PDF...")
        
        # Extract pages
        print("   Extracting pages...")
        pages = self.extract_text_with_structure()
        print(f"   ✓ Extracted {len(pages)} pages")
        
        # Detect structure
        print("   Detecting document structure...")
        structured = self.detect_structure(pages)
        
        headers = [e for e in structured if e['type'] == 'header']
        print(f"   ✓ Found {len(headers)} headers (staging sections)")
        
        # Create semantic chunks
        print("   Creating semantic chunks...")
        raw_chunks = self.create_semantic_chunks(structured)
        print(f"   ✓ Created {len(raw_chunks)} raw chunks")
        
        # Convert to Chunk objects
        for raw_chunk in raw_chunks:
            category = self._identify_category(raw_chunk.get('section', ''))
            
            chunk = Chunk(
                chunk_id=f"tnm_lung_{raw_chunk['id']}",
                text=raw_chunk['text'],
                source_type="tnm_lung_protocol",
                metadata={
                    "source_file": self.pdf_path.name,
                    "created_at": datetime.now().isoformat(),
                    "page": raw_chunk['page'],
                    "cancer_type": "Lung",
                    "category": category,
                    "section": raw_chunk.get('section'),
                    "subsection": raw_chunk.get('subsection'),
                    "tnm_edition": "9th",
                    "protocol_type": "staging_documentation",
                    "chunk_method": "semantic",
                    "char_count": raw_chunk['char_count']
                }
            )
            
            self.chunks.append(chunk)
        
        print(f"   ✓ Converted to {len(self.chunks)} Chunk objects")
        
        return self.chunks
    
    def save_chunks(self, output_path: Path):
        """Save chunks to JSONL"""
        print(f"   Saving to: {output_path}")
        
        with open(output_path, 'w', encoding='utf-8') as f:
            for chunk in self.chunks:
                f.write(json.dumps(chunk.to_dict()) + '\n')
        
        print(f"   ✓ Saved {len(self.chunks):,} chunks")


class RECISTProcessor:
    """Process RECIST Guidelines PDF into structured chunks"""
    
    def __init__(self, pdf_path: Path):
        self.pdf_path = pdf_path
        self.chunks: List[Chunk] = []
    
    def extract_text_with_structure(self) -> List[Dict]:
        """Extract text from PDF preserving page structure"""
        if not PYMUPDF_AVAILABLE:
            raise ImportError("PyMuPDF required. Install: pip install pymupdf")
        
        pages = []
        doc = fitz.open(self.pdf_path)
        
        for page_num, page in enumerate(doc, start=1):
            pages.append({
                'page_number': page_num,
                'text': page.get_text()
            })
        
        doc.close()
        return pages
    
    def detect_structure(self, pages: List[Dict]) -> List[Dict]:
        """Detect RECIST document structure (sections and subsections)"""
        structured_elements = []
        
        for page in pages:
            page_num = page['page_number']
            text = page['text']
            lines = text.split('\n')
            
            current_section = None
            current_subsection = None
            buffer = []
            
            for i, line in enumerate(lines):
                line_stripped = line.strip()
                
                if not line_stripped:
                    if buffer and buffer[-1]:
                        buffer.append('')
                    continue
                
                # Detect headers
                header_info = self._detect_recist_header(line_stripped)
                
                if header_info:
                    # Save previous buffer
                    if buffer:
                        while buffer and not buffer[-1]:
                            buffer.pop()
                        
                        if buffer:
                            structured_elements.append({
                                'type': 'content',
                                'text': '\n'.join(buffer),
                                'page': page_num,
                                'section': current_section,
                                'subsection': current_subsection
                            })
                        buffer = []
                    
                    # Add header
                    structured_elements.append({
                        'type': 'header',
                        'level': header_info['level'],
                        'text': line_stripped,
                        'page': page_num
                    })
                    
                    # Update context
                    if header_info['level'] == 1:
                        current_section = line_stripped
                        current_subsection = None
                    elif header_info['level'] == 2:
                        current_subsection = line_stripped
                else:
                    buffer.append(line_stripped)
            
            # Save remaining buffer
            if buffer:
                while buffer and not buffer[-1]:
                    buffer.pop()
                
                if buffer:
                    structured_elements.append({
                        'type': 'content',
                        'text': '\n'.join(buffer),
                        'page': page_num,
                        'section': current_section,
                        'subsection': current_subsection
                    })
        
        return structured_elements
    
    def _detect_recist_header(self, line: str) -> Dict:
        """Detect RECIST-specific headers"""
        
        # Numbered sections
        numbered_match = re.match(r'^(\d+\.)+\s+([A-Z][a-zA-Z].+)', line)
        if numbered_match:
            if not line.endswith('.') or len(line) < 80:
                level = min(numbered_match.group(1).count('.'), 3)
                return {'level': level, 'pattern': 'numbered'}
        
        # ALL CAPS headers
        if (line.isupper() and 
            2 <= len(line.split()) <= 8 and
            len(line) < 80 and
            not line.endswith('.')):
            return {'level': 1, 'pattern': 'all_caps'}
        
        # Key RECIST section keywords
        keywords = [
            'INTRODUCTION', 'BACKGROUND', 'METHODS', 'ASSESSMENT', 'MEASUREMENT',
            'TARGET LESIONS', 'NON-TARGET LESIONS', 'RESPONSE CRITERIA',
            'COMPLETE RESPONSE', 'PARTIAL RESPONSE', 'PROGRESSIVE DISEASE',
            'STABLE DISEASE'
        ]
        
        line_upper = line.upper().strip()
        if line_upper in keywords:
            return {'level': 1, 'pattern': 'keyword'}
        
        return None
    
    def create_semantic_chunks(self, structured_elements: List[Dict]) -> List[Dict]:
        """Create chunks by semantic sections"""
        chunks = []
        chunk_id = 0
        
        i = 0
        while i < len(structured_elements):
            element = structured_elements[i]
            
            if element['type'] == 'header':
                section_header = element['text']
                section_level = element['level']
                section_page = element['page']
                
                section_content = []
                section_subsection = None
                i += 1
                
                while i < len(structured_elements):
                    next_elem = structured_elements[i]
                    
                    if next_elem['type'] == 'header':
                        if next_elem['level'] <= section_level:
                            break
                        elif next_elem['level'] == section_level + 1:
                            section_subsection = next_elem['text']
                    
                    if next_elem['type'] == 'content':
                        section_content.append(next_elem['text'])
                    
                    i += 1
                
                if section_content:
                    full_section_text = f"{section_header}\n\n" + '\n\n'.join(section_content)
                    
                    if len(full_section_text) <= 3000:
                        chunks.append({
                            'id': f"chunk_{chunk_id}",
                            'text': full_section_text,
                            'page': section_page,
                            'section': section_header,
                            'subsection': section_subsection,
                            'char_count': len(full_section_text)
                        })
                        chunk_id += 1
                    else:
                        # Split intelligently
                        paragraphs = full_section_text.split('\n\n')
                        current_chunk = [section_header]
                        current_length = len(section_header) + 2
                        
                        for para in paragraphs[1:]:
                            para_len = len(para)
                            
                            if current_length + para_len + 2 > 3000:
                                if len(current_chunk) > 1:
                                    chunks.append({
                                        'id': f"chunk_{chunk_id}",
                                        'text': '\n\n'.join(current_chunk),
                                        'page': section_page,
                                        'section': section_header,
                                        'subsection': section_subsection,
                                        'char_count': current_length
                                    })
                                    chunk_id += 1
                                
                                current_chunk = [section_header, para]
                                current_length = len(section_header) + para_len + 4
                            else:
                                current_chunk.append(para)
                                current_length += para_len + 2
                        
                        if len(current_chunk) > 1:
                            chunks.append({
                                'id': f"chunk_{chunk_id}",
                                'text': '\n\n'.join(current_chunk),
                                'page': section_page,
                                'section': section_header,
                                'subsection': section_subsection,
                                'char_count': current_length
                            })
                            chunk_id += 1
            else:
                if element['type'] == 'content':
                    chunks.append({
                        'id': f"chunk_{chunk_id}",
                        'text': element['text'],
                        'page': element.get('page', 1),
                        'section': None,
                        'subsection': None,
                        'char_count': len(element['text'])
                    })
                    chunk_id += 1
                i += 1
        
        return chunks
    
    def process(self) -> List[Chunk]:
        """Process RECIST PDF into chunks"""
        print("\n   Processing RECIST 1.1 PDF...")
        
        # Extract pages
        print("   Extracting pages...")
        pages = self.extract_text_with_structure()
        print(f"   ✓ Extracted {len(pages)} pages")
        
        # Detect structure
        print("   Detecting document structure...")
        structured = self.detect_structure(pages)
        
        headers = [e for e in structured if e['type'] == 'header']
        print(f"   ✓ Found {len(headers)} headers")
        
        # Create semantic chunks
        print("   Creating semantic chunks...")
        raw_chunks = self.create_semantic_chunks(structured)
        print(f"   ✓ Created {len(raw_chunks)} raw chunks")
        
        # Convert to Chunk objects
        for raw_chunk in raw_chunks:
            chunk = Chunk(
                chunk_id=f"recist_{raw_chunk['id']}",
                text=raw_chunk['text'],
                source_type="recist",
                metadata={
                    "source_file": self.pdf_path.name,
                    "created_at": datetime.now().isoformat(),
                    "page": raw_chunk['page'],
                    "section": raw_chunk.get('section'),
                    "subsection": raw_chunk.get('subsection'),
                    "chunk_method": "semantic",
                    "char_count": raw_chunk['char_count']
                }
            )
            
            self.chunks.append(chunk)
        
        print(f"   ✓ Converted to {len(self.chunks)} Chunk objects")
        
        return self.chunks
    
    def save_chunks(self, output_path: Path):
        """Save chunks to JSONL"""
        print(f"   Saving to: {output_path}")
        
        with open(output_path, 'w', encoding='utf-8') as f:
            for chunk in self.chunks:
                f.write(json.dumps(chunk.to_dict()) + '\n')
        
        print(f"   ✓ Saved {len(self.chunks):,} chunks")

