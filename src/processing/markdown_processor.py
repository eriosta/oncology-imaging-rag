"""
Markdown Processing Strategy:
- Parse structured markdown (tables, headers)
- Create chunks by logical sections
- Preserve table formatting
- Maintain hierarchical context
"""

from pathlib import Path
from typing import List, Dict, Tuple
import json
from datetime import datetime
import re

import sys
sys.path.append(str(Path(__file__).parent.parent))

from models.chunk import Chunk


class MarkdownProcessor:
    """Process structured markdown files into chunks"""
    
    def __init__(self, markdown_path: Path):
        self.markdown_path = markdown_path
        self.chunks: List[Chunk] = []
    
    def load_markdown(self) -> str:
        """Load markdown content"""
        print(f"   Loading markdown from: {self.markdown_path.name}")
        
        if not self.markdown_path.exists():
            raise FileNotFoundError(f"Markdown file not found: {self.markdown_path}")
        
        with open(self.markdown_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        return content
    
    def parse_markdown_structure(self, content: str) -> List[Dict]:
        """Parse markdown into structured elements (headers, tables, text)"""
        lines = content.split('\n')
        elements = []
        
        i = 0
        while i < len(lines):
            line = lines[i]
            
            # Detect headers
            if line.startswith('#'):
                level = len(line) - len(line.lstrip('#'))
                text = line.lstrip('#').strip()
                elements.append({
                    'type': 'header',
                    'level': level,
                    'text': text,
                    'line_num': i + 1
                })
                i += 1
            
            # Detect tables (markdown tables have | separators)
            elif '|' in line and line.strip():
                # Start of table - collect all table lines
                table_lines = []
                table_start = i
                
                while i < len(lines) and ('|' in lines[i] or not lines[i].strip()):
                    if '|' in lines[i]:
                        table_lines.append(lines[i])
                    i += 1
                
                if table_lines:
                    elements.append({
                        'type': 'table',
                        'lines': table_lines,
                        'text': '\n'.join(table_lines),
                        'line_num': table_start + 1
                    })
            
            # Regular text
            elif line.strip():
                # Collect consecutive text lines
                text_lines = [line]
                text_start = i
                i += 1
                
                while i < len(lines) and lines[i].strip() and not lines[i].startswith('#') and '|' not in lines[i]:
                    text_lines.append(lines[i])
                    i += 1
                
                elements.append({
                    'type': 'text',
                    'text': '\n'.join(text_lines),
                    'line_num': text_start + 1
                })
            else:
                i += 1
        
        return elements
    
    def create_semantic_chunks(self, elements: List[Dict]) -> List[Dict]:
        """Create chunks based on semantic sections"""
        chunks = []
        chunk_id = 0
        
        current_section = None
        current_subsection = None
        current_subsubsection = None
        
        i = 0
        while i < len(elements):
            element = elements[i]
            
            if element['type'] == 'header':
                level = element['level']
                text = element['text']
                
                # Update context based on header level
                if level == 1:
                    current_section = text
                    current_subsection = None
                    current_subsubsection = None
                elif level == 2:
                    current_subsection = text
                    current_subsubsection = None
                elif level == 3:
                    current_subsubsection = text
                elif level == 4:
                    # Station headers - create chunks with content
                    station_header = text
                    content_parts = [f"### {station_header}"]
                    
                    # Collect content until next header of same or higher level
                    i += 1
                    while i < len(elements):
                        next_elem = elements[i]
                        
                        if next_elem['type'] == 'header' and next_elem['level'] <= 4:
                            break
                        
                        if next_elem['type'] == 'table':
                            content_parts.append(next_elem['text'])
                        elif next_elem['type'] == 'text':
                            content_parts.append(next_elem['text'])
                        
                        i += 1
                    
                    # Create chunk for this station
                    if len(content_parts) > 1:
                        chunks.append({
                            'id': f"chunk_{chunk_id}",
                            'text': '\n\n'.join(content_parts),
                            'section': current_section,
                            'subsection': current_subsection,
                            'subsubsection': current_subsubsection,
                            'station': station_header,
                            'char_count': len('\n\n'.join(content_parts))
                        })
                        chunk_id += 1
                    
                    continue  # Already incremented i in the loop
                
                i += 1
            
            elif element['type'] == 'table':
                # For tables not under a level-4 header, create standalone chunk
                table_text = element['text']
                
                # Add context header
                context_parts = []
                if current_subsubsection:
                    context_parts.append(f"### {current_subsubsection}")
                elif current_subsection:
                    context_parts.append(f"## {current_subsection}")
                
                context_parts.append(table_text)
                
                chunks.append({
                    'id': f"chunk_{chunk_id}",
                    'text': '\n\n'.join(context_parts),
                    'section': current_section,
                    'subsection': current_subsection,
                    'subsubsection': current_subsubsection,
                    'station': None,
                    'char_count': len('\n\n'.join(context_parts))
                })
                chunk_id += 1
                i += 1
            
            elif element['type'] == 'text':
                # For standalone text, only create chunk if substantial
                text = element['text']
                if len(text) > 50:  # Minimum threshold
                    context_parts = []
                    if current_subsubsection:
                        context_parts.append(f"### {current_subsubsection}")
                    context_parts.append(text)
                    
                    chunks.append({
                        'id': f"chunk_{chunk_id}",
                        'text': '\n\n'.join(context_parts),
                        'section': current_section,
                        'subsection': current_subsection,
                        'subsubsection': current_subsubsection,
                        'station': None,
                        'char_count': len('\n\n'.join(context_parts))
                    })
                    chunk_id += 1
                i += 1
            else:
                i += 1
        
        return chunks
    
    def identify_cancer_type(self, text: str) -> str:
        """Identify cancer type from text"""
        if not text:
            return "Lung"  # Default for this TNM file
        
        text_lower = text.lower()
        cancer_types = {
            'lung': 'Lung',
            'thyroid': 'Thyroid',
            'breast': 'Breast',
            'colon': 'Colon',
            'prostate': 'Prostate',
            'liver': 'Liver',
            'kidney': 'Kidney',
        }
        
        for keyword, cancer_type in cancer_types.items():
            if keyword in text_lower:
                return cancer_type
        
        return "Lung"  # Default
    
    def identify_category(self, text: str) -> str:
        """Identify staging category from text"""
        if not text:
            return "Staging"
        
        text_upper = text.upper()
        
        if 'NODAL' in text_upper or 'NODE' in text_upper or 'STATION' in text_upper:
            return 'N-staging'
        elif 'STAGE GROUP' in text_upper or 'TNM' in text_upper:
            return 'TNM-staging'
        elif 'METASTASIS' in text_upper or 'M1' in text_upper:
            return 'M-staging'
        
        return 'Staging'
    
    def process(self) -> List[Chunk]:
        """Process markdown into chunks"""
        print("\n   Processing TNM markdown file...")
        
        # Load content
        content = self.load_markdown()
        print(f"   ✓ Loaded {len(content):,} characters")
        
        # Parse structure
        print("   Parsing markdown structure...")
        elements = self.parse_markdown_structure(content)
        
        headers = [e for e in elements if e['type'] == 'header']
        tables = [e for e in elements if e['type'] == 'table']
        print(f"   ✓ Found {len(headers)} headers, {len(tables)} tables")
        
        # Create semantic chunks
        print("   Creating semantic chunks...")
        raw_chunks = self.create_semantic_chunks(elements)
        print(f"   ✓ Created {len(raw_chunks)} raw chunks")
        
        # Convert to Chunk objects
        for raw_chunk in raw_chunks:
            cancer_type = self.identify_cancer_type(
                raw_chunk.get('section', '') or raw_chunk.get('text', '')
            )
            category = self.identify_category(
                raw_chunk.get('subsection', '') or raw_chunk.get('text', '')
            )
            
            chunk = Chunk(
                chunk_id=f"tnm_md_{raw_chunk['id']}",
                text=raw_chunk['text'],
                source_type="tnm_markdown",
                metadata={
                    "source_file": self.markdown_path.name,
                    "created_at": datetime.now().isoformat(),
                    "cancer_type": cancer_type,
                    "category": category,
                    "section": raw_chunk.get('section'),
                    "subsection": raw_chunk.get('subsection'),
                    "subsubsection": raw_chunk.get('subsubsection'),
                    "station": raw_chunk.get('station'),
                    "tnm_edition": "9th",
                    "chunk_method": "semantic_markdown",
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


class TNMMarkdownProcessor(MarkdownProcessor):
    """Specialized processor for TNM markdown files"""
    
    def __init__(self, markdown_path: Path):
        super().__init__(markdown_path)
    
    def process(self) -> List[Chunk]:
        """Process TNM markdown with enhanced logic"""
        print("\n   Processing TNM 9th Edition markdown...")
        
        # Load content
        content = self.load_markdown()
        print(f"   ✓ Loaded {len(content):,} characters")
        
        # Parse structure
        print("   Parsing markdown structure...")
        elements = self.parse_markdown_structure(content)
        
        headers = [e for e in elements if e['type'] == 'header']
        tables = [e for e in elements if e['type'] == 'table']
        print(f"   ✓ Found {len(headers)} headers, {len(tables)} tables")
        
        # Create semantic chunks
        print("   Creating semantic chunks...")
        raw_chunks = self.create_semantic_chunks(elements)
        print(f"   ✓ Created {len(raw_chunks)} raw chunks")
        
        # Convert to Chunk objects with TNM-specific metadata
        for raw_chunk in raw_chunks:
            cancer_type = "Lung"  # This file is specifically for lung cancer
            
            # Determine category based on content
            text = raw_chunk['text']
            station = raw_chunk.get('station') or ''
            section = raw_chunk.get('section') or ''
            subsection = raw_chunk.get('subsection') or ''
            
            if 'Station' in station or 'Nodal' in section:
                category = 'N-staging'
                subcategory = 'Lymph Node Stations'
            elif 'Stage Group' in subsection:
                category = 'TNM-staging'
                subcategory = 'Stage Classification'
            else:
                category = 'Staging'
                subcategory = None
            
            chunk = Chunk(
                chunk_id=f"tnm_md_{raw_chunk['id']}",
                text=raw_chunk['text'],
                source_type="tnm_markdown",
                metadata={
                    "source_file": self.markdown_path.name,
                    "created_at": datetime.now().isoformat(),
                    "cancer_type": cancer_type,
                    "category": category,
                    "subcategory": subcategory,
                    "section": raw_chunk.get('section'),
                    "subsection": raw_chunk.get('subsection'),
                    "subsubsection": raw_chunk.get('subsubsection'),
                    "station": raw_chunk.get('station'),
                    "tnm_edition": "9th",
                    "chunk_method": "semantic_markdown",
                    "char_count": raw_chunk['char_count']
                }
            )
            
            self.chunks.append(chunk)
        
        print(f"   ✓ Converted to {len(self.chunks)} Chunk objects")
        
        return self.chunks

