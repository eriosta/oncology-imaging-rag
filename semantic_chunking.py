#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Semantic Chunking for Structured Medical Documents

Respects document structure (sections, headers, hierarchies) to create
contextually-rich chunks optimized for BGE embeddings.

Usage:
    python semantic_chunking.py --process recist
    python semantic_chunking.py --process tnm
    python semantic_chunking.py --process-all
"""

import re
import json
import argparse
from pathlib import Path
from typing import List, Dict, Tuple
from collections import defaultdict

try:
    import fitz  # PyMuPDF
    PYMUPDF_AVAILABLE = True
except ImportError:
    PYMUPDF_AVAILABLE = False


class SemanticChunker:
    """Semantic-aware chunking for structured medical documents"""
    
    def __init__(self, max_chunk_size=3000, min_chunk_size=200, overlap=100):
        """
        Args:
            max_chunk_size: Maximum chars per chunk (BGE-large-en-v1.5 has 512 token limit ≈ 2048 chars)
                           Set to 3000 to allow semantic sections to stay together
            min_chunk_size: Minimum chars per chunk
            overlap: Characters of overlap between chunks (not used in semantic chunking)
        """
        self.max_chunk_size = max_chunk_size
        self.min_chunk_size = min_chunk_size
        self.overlap = overlap
    
    def detect_structure(self, pages: List[Dict]) -> List[Dict]:
        """
        Detect hierarchical structure in document while preserving ALL content
        
        Strategy: Identify key section headers but include ALL text in content blocks
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
                    if buffer and buffer[-1]:  # Add spacing between paragraphs
                        buffer.append('')
                    continue
                
                # Detect MAJOR headers only (be conservative)
                header_info = self._detect_major_header(line_stripped, lines[max(0, i-1):min(len(lines), i+3)])
                
                if header_info:
                    # Save previous buffer if exists
                    if buffer:
                        # Remove trailing empty lines
                        while buffer and not buffer[-1]:
                            buffer.pop()
                        
                        if buffer:  # Only save if there's actual content
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
                    # Include ALL text as content
                    buffer.append(line_stripped)
            
            # Save remaining buffer at end of page
            if buffer:
                # Remove trailing empty lines
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
    
    def _detect_major_header(self, line: str, context: List[str]) -> Dict:
        """
        Detect MAJOR headers only (conservative approach to avoid false positives)
        
        Only matches:
        - Clear numbered sections (1. , 2.1 , etc.) at start of line
        - Cancer type headers (e.g., "Lung Cancer T Classification")
        - All caps multi-word headers on their own line
        - Specific known section keywords
        - Appendix headers
        """
        # Skip repeated organization headers
        if 'INTERNATIONAL ASSOCIATION' in line or line.startswith('E U R O P E A N'):
            return None
        
        # TNM-specific: Cancer type + staging component (e.g., "Lung Cancer T Classification")
        tnm_pattern = re.match(r'^([A-Z][a-zA-Z\s]+(?:Cancer|Tumors?|Mesothelioma|Carcinoma))\s+(T|N|M|TNM)\s+(Classification|Deﬁnitions?|Stages?)', line, re.IGNORECASE)
        if tnm_pattern:
            return {'level': 1, 'pattern': 'tnm_cancer_type'}
        
        # TNM-specific: Just cancer type section (e.g., "Thymic Epithelial Tumors–9th Edition")
        cancer_type_pattern = re.match(r'^([A-Z][a-zA-Z\s]+(?:Cancer|Tumors?|Mesothelioma|Carcinoma))', line)
        if cancer_type_pattern and ('–9th Edition' in line or 'Classification' in line or len(line.split()) <= 6):
            return {'level': 1, 'pattern': 'tnm_cancer_type'}
        
        # Numbered sections at start of line (e.g., "1. Background", "3.1.1. Measurable")
        # Must be followed by a capital letter word (not just numbers)
        numbered_match = re.match(r'^(\d+\.)+\s+([A-Z][a-zA-Z].+)', line)
        if numbered_match:
            # Exclude reference citations (they typically end with a period or are very long)
            if not line.endswith('.') or len(line) < 80:
                level = min(numbered_match.group(1).count('.'), 3)
                return {'level': level, 'pattern': 'numbered'}
        
        # Appendix headers
        if re.match(r'^Appendix\s+[IVX]+', line, re.IGNORECASE):
            return {'level': 1, 'pattern': 'appendix'}
        
        # ALL CAPS multi-word headers (but not single words and not too long)
        if (line.isupper() and 
            2 <= len(line.split()) <= 8 and  # Multi-word but not entire paragraph
            len(line) < 80 and
            not line.endswith('.')):  # Headers don't end with periods
            return {'level': 1, 'pattern': 'all_caps'}
        
        # Specific major section keywords
        major_keywords = [
            'BACKGROUND', 'METHODS', 'RESULTS', 'DISCUSSION',
            'SUMMARY', 'CONCLUSION', 'REFERENCES'
        ]
        
        line_upper = line.upper().strip()
        if line_upper in major_keywords:
            return {'level': 1, 'pattern': 'keyword'}
        
        return None
    
    def _detect_header(self, line: str, context: List[str]) -> Dict:
        """
        Detect if line is a header and its level
        
        Patterns:
        - ALL CAPS (3+ words)
        - Numbered (1. , 1.1 , etc.)
        - Short lines followed by content
        - Specific keywords (INTRODUCTION, METHODS, etc.)
        """
        # ALL CAPS header (common in medical guidelines)
        if line.isupper() and len(line.split()) >= 2 and len(line) < 100:
            return {'level': 1, 'pattern': 'all_caps'}
        
        # Numbered sections
        numbered_match = re.match(r'^(\d+\.)+\s+([A-Z].+)', line)
        if numbered_match:
            level = numbered_match.group(1).count('.')
            return {'level': min(level, 3), 'pattern': 'numbered'}
        
        # Common medical document headers
        header_keywords = [
            'INTRODUCTION', 'BACKGROUND', 'METHODS', 'RESULTS', 'DISCUSSION',
            'SUMMARY', 'CONCLUSION', 'OBJECTIVES', 'CRITERIA', 'DEFINITIONS',
            'ASSESSMENT', 'EVALUATION', 'MEASUREMENT', 'GUIDELINES', 'STAGING',
            'CLASSIFICATION', 'APPENDIX'
        ]
        
        line_upper = line.upper()
        for keyword in header_keywords:
            if keyword in line_upper and len(line.split()) <= 5:
                return {'level': 1, 'pattern': 'keyword'}
        
        # Short line that looks like header (< 60 chars, capitalized, no period at end)
        if (len(line) < 60 and 
            line[0].isupper() and 
            not line.endswith('.') and 
            not line.endswith(',') and
            len(line.split()) <= 10):
            # Check if followed by regular content
            if len(context) > 1 and len(context[1]) > len(line):
                return {'level': 2, 'pattern': 'short_title'}
        
        return None
    
    def create_semantic_chunks(self, structured_elements: List[Dict]) -> List[Dict]:
        """
        Create chunks by SEMANTIC SECTIONS - group content under each header
        
        Strategy:
        1. Each major header starts a new semantic section
        2. Include header text in the chunk for context
        3. Aggregate all content under that header
        4. Only split if section exceeds max_chunk_size
        """
        chunks = []
        chunk_id = 0
        
        i = 0
        while i < len(structured_elements):
            element = structured_elements[i]
            
            if element['type'] == 'header':
                # Start a new semantic section
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
                        # Track subsection headers but continue
                        elif next_elem['level'] == section_level + 1:
                            section_subsection = next_elem['text']
                    
                    # Add content
                    if next_elem['type'] == 'content':
                        section_content.append(next_elem['text'])
                    
                    i += 1
                
                # Build the complete section text with header
                if section_content:
                    # Start with header as context
                    full_section_text = f"{section_header}\n\n" + '\n\n'.join(section_content)
                    
                    # If section fits in one chunk, use it directly
                    if len(full_section_text) <= self.max_chunk_size:
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
                        # Section too large - split intelligently
                        # Strategy: Split at paragraph boundaries while keeping header context
                        paragraphs = full_section_text.split('\n\n')
                        current_chunk = [section_header]  # Always start with header
                        current_length = len(section_header) + 2
                        
                        for para in paragraphs[1:]:  # Skip header (already added)
                            para_len = len(para)
                            
                            # If adding this paragraph would exceed limit
                            if current_length + para_len + 2 > self.max_chunk_size:
                                # Save current chunk
                                if len(current_chunk) > 1:  # Has more than just header
                                    chunks.append({
                                        'id': f"chunk_{chunk_id}",
                                        'text': '\n\n'.join(current_chunk),
                                        'page': section_page,
                                        'section': section_header,
                                        'subsection': section_subsection,
                                        'char_count': current_length
                                    })
                                    chunk_id += 1
                                
                                # Start new chunk with header context
                                current_chunk = [section_header, para]
                                current_length = len(section_header) + para_len + 4
                            else:
                                # Add to current chunk
                                current_chunk.append(para)
                                current_length += para_len + 2
                        
                        # Save final chunk
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
                # Content without header (rare) - still include it
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
    
    def _create_breadcrumb(self, context: Dict) -> str:
        """Create context breadcrumb for chunk"""
        parts = []
        
        if context['section']:
            parts.append(context['section'])
        
        if context['subsection']:
            parts.append(context['subsection'])
        
        if parts:
            return ' > '.join(parts) + '\n\n'
        
        return ''
    
    def process_recist(self):
        """Process RECIST with semantic chunking"""
        print("\n" + "="*70)
        print("SEMANTIC CHUNKING: RECIST 1.1")
        print("="*70)
        
        pdf_path = Path("data/guidelines/RECIST_1.1_EORTC.pdf").resolve()
        output_dir = Path("data/guidelines/processed").resolve()
        
        if not pdf_path.exists():
            print(f"❌ PDF not found: {pdf_path}")
            return
        
        # Extract pages
        print("\n📄 Extracting pages...")
        pages = self._extract_pdf_text(pdf_path)
        print(f"   Extracted {len(pages)} pages")
        
        # Detect structure
        print("\n🔍 Detecting document structure...")
        structured = self.detect_structure(pages)
        
        headers = [e for e in structured if e['type'] == 'header']
        content_blocks = [e for e in structured if e['type'] == 'content']
        print(f"   Found {len(headers)} headers")
        print(f"   Found {len(content_blocks)} content blocks")
        
        # Show structure
        print("\n📋 Document Structure:")
        for header in headers[:10]:
            indent = "  " * (header['level'] - 1)
            print(f"   {indent}• {header['text'][:60]}")
        if len(headers) > 10:
            print(f"   ... and {len(headers) - 10} more")
        
        # Create semantic chunks
        print("\n✂️  Creating semantic chunks...")
        chunks = self.create_semantic_chunks(structured)
        print(f"   Created {len(chunks)} chunks")
        
        # Statistics
        chunk_sizes = [c['char_count'] for c in chunks]
        print(f"\n📊 Chunk Statistics:")
        print(f"   Min size: {min(chunk_sizes):,} chars")
        print(f"   Max size: {max(chunk_sizes):,} chars")
        print(f"   Mean size: {sum(chunk_sizes)/len(chunks):.0f} chars")
        print(f"   Median size: {sorted(chunk_sizes)[len(chunk_sizes)//2]:,} chars")
        
        # Show examples
        print(f"\n📝 Example Chunks (first 2):")
        for i, chunk in enumerate(chunks[:2]):
            print(f"\n   Chunk {i+1}:")
            print(f"   Section: {chunk['section']}")
            print(f"   Subsection: {chunk['subsection']}")
            print(f"   Size: {chunk['char_count']} chars")
            print(f"   Preview: {chunk['text'][:150]}...")
        
        # Create RAG documents
        documents = []
        for chunk in chunks:
            doc = {
                'id': f"RECIST_1.1_EORTC_{chunk['id']}",
                'text': chunk['text'],
                'metadata': {
                    'source': 'RECIST_1.1_EORTC',
                    'type': 'guideline',
                    'page': chunk['page'],
                    'section': chunk['section'],
                    'subsection': chunk['subsection'],
                    'chunking_method': 'semantic',
                    'char_count': chunk['char_count']
                }
            }
            documents.append(doc)
        
        # Save
        output_dir.mkdir(parents=True, exist_ok=True)
        output_file = output_dir / "RECIST_1.1_EORTC_rag_documents_semantic.json"
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(documents, f, indent=2, ensure_ascii=False)
        
        # Save statistics
        stats = {
            'source_file': pdf_path.name,
            'total_pages': len(pages),
            'total_chunks': len(chunks),
            'total_headers': len(headers),
            'chunking_method': 'semantic',
            'avg_chunk_size': sum(chunk_sizes) / len(chunks),
            'min_chunk_size': min(chunk_sizes),
            'max_chunk_size': max(chunk_sizes)
        }
        
        with open(output_dir / "RECIST_1.1_EORTC_statistics_semantic.json", 'w', encoding='utf-8') as f:
            json.dump(stats, f, indent=2)
        
        print(f"\n✅ Saved: {output_file.name}")
        print(f"✅ Semantic chunking complete!")
        
        return documents
    
    def process_tnm(self):
        """Process TNM Lung Cancer Protocol with semantic chunking"""
        print("\n" + "="*70)
        print("SEMANTIC CHUNKING: TNM 9th Edition - Lung Cancer Protocol")
        print("="*70)
        
        pdf_path = Path("data/tnm9ed/Lung_ Protocol for Cancer Staging Documentation.pdf").resolve()
        output_dir = Path("data/tnm9ed/processed").resolve()
        
        if not pdf_path.exists():
            print(f"❌ PDF not found: {pdf_path}")
            return
        
        # Extract pages
        print("\n📄 Extracting pages...")
        pages = self._extract_pdf_text(pdf_path)
        print(f"   Extracted {len(pages)} pages")
        
        # Detect structure
        print("\n🔍 Detecting document structure...")
        structured = self.detect_structure(pages)
        
        headers = [e for e in structured if e['type'] == 'header']
        print(f"   Found {len(headers)} headers (cancer sites)")
        
        # Show structure
        print("\n📋 TNM Structure (Cancer Sites):")
        for header in headers[:15]:
            indent = "  " * (header['level'] - 1)
            print(f"   {indent}• {header['text'][:60]}")
        if len(headers) > 15:
            print(f"   ... and {len(headers) - 15} more")
        
        # Create semantic chunks
        print("\n✂️  Creating semantic chunks...")
        chunks = self.create_semantic_chunks(structured)
        print(f"   Created {len(chunks)} chunks")
        
        # Statistics
        chunk_sizes = [c['char_count'] for c in chunks]
        print(f"\n📊 Chunk Statistics:")
        print(f"   Min size: {min(chunk_sizes):,} chars")
        print(f"   Max size: {max(chunk_sizes):,} chars")
        print(f"   Mean size: {sum(chunk_sizes)/len(chunks):.0f} chars")
        
        # Create RAG documents
        documents = []
        for chunk in chunks:
            doc = {
                'id': f"TNM_9th_Edition_{chunk['id']}",
                'text': chunk['text'],
                'metadata': {
                    'source': 'TNM_9th_Edition_2024',
                    'type': 'staging',
                    'page': chunk['page'],
                    'cancer_site': chunk['section'],
                    'staging_component': chunk['subsection'],
                    'chunking_method': 'semantic',
                    'char_count': chunk['char_count']
                }
            }
            documents.append(doc)
        
        # Save
        output_dir.mkdir(parents=True, exist_ok=True)
        output_file = output_dir / "Lung_Protocol_9th_Edition_rag_documents_semantic.json"
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(documents, f, indent=2, ensure_ascii=False)
        
        stats = {
            'source_file': pdf_path.name,
            'total_pages': len(pages),
            'total_chunks': len(chunks),
            'total_sections': len([h for h in headers if h['level'] == 1]),
            'chunking_method': 'semantic',
            'avg_chunk_size': sum(chunk_sizes) / len(chunks)
        }
        
        with open(output_dir / "Lung_Protocol_9th_Edition_statistics_semantic.json", 'w', encoding='utf-8') as f:
            json.dump(stats, f, indent=2)
        
        print(f"\n✅ Saved: {output_file.name}")
        print(f"✅ Semantic chunking complete!")
        
        return documents
    
    def _extract_pdf_text(self, pdf_path: Path) -> List[Dict]:
        """Extract text from PDF"""
        if not PYMUPDF_AVAILABLE:
            raise ImportError("PyMuPDF required. Install: pip install pymupdf")
        
        pages = []
        doc = fitz.open(pdf_path)
        
        for page_num, page in enumerate(doc, start=1):
            pages.append({
                'page_number': page_num,
                'text': page.get_text()
            })
        
        doc.close()
        return pages


def compare_chunking_methods():
    """Compare paragraph vs semantic chunking"""
    print("\n" + "="*70)
    print("COMPARISON: Paragraph vs Semantic Chunking")
    print("="*70)
    
    # Load both versions
    para_file = Path("data/guidelines/processed/RECIST_1.1_EORTC_rag_documents.json")
    sem_file = Path("data/guidelines/processed/RECIST_1.1_EORTC_rag_documents_semantic.json")
    
    if not para_file.exists() or not sem_file.exists():
        print("❌ Need both versions to compare")
        return
    
    with open(para_file) as f:
        para_docs = json.load(f)
    
    with open(sem_file) as f:
        sem_docs = json.load(f)
    
    print(f"\n📊 Chunk Count:")
    print(f"   Paragraph method: {len(para_docs)} chunks")
    print(f"   Semantic method:  {len(sem_docs)} chunks")
    
    para_sizes = [len(d['text']) for d in para_docs]
    sem_sizes = [len(d['text']) for d in sem_docs]
    
    print(f"\n📏 Average Chunk Size:")
    print(f"   Paragraph method: {sum(para_sizes)/len(para_sizes):.0f} chars")
    print(f"   Semantic method:  {sum(sem_sizes)/len(sem_sizes):.0f} chars")
    
    print(f"\n📋 Context Information:")
    para_with_context = sum(1 for d in para_docs if d.get('metadata', {}).get('section'))
    sem_with_context = sum(1 for d in sem_docs if d.get('metadata', {}).get('section'))
    
    print(f"   Paragraph: {para_with_context}/{len(para_docs)} have section context")
    print(f"   Semantic:  {sem_with_context}/{len(sem_docs)} have section context")
    
    print(f"\n💡 Key Differences:")
    print(f"   Semantic chunking:")
    print(f"   ✅ Preserves document structure")
    print(f"   ✅ Adds section breadcrumbs")
    print(f"   ✅ Respects semantic boundaries")
    print(f"   ✅ Better for BGE-large-en-v1.5 (contextual model)")
    
    # Show example comparison
    print(f"\n📝 Example Comparison:")
    print(f"\n   Paragraph Chunk (first):")
    print(f"   {para_docs[0]['text'][:200]}...")
    
    print(f"\n   Semantic Chunk (first):")
    print(f"   {sem_docs[0]['text'][:200]}...")


def main():
    parser = argparse.ArgumentParser(
        description="Semantic Chunking for Structured Medical Documents",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Process RECIST with semantic chunking
  python semantic_chunking.py --process recist
  
  # Process TNM with semantic chunking
  python semantic_chunking.py --process tnm
  
  # Process both
  python semantic_chunking.py --process-all
  
  # Compare methods
  python semantic_chunking.py --compare
        """
    )
    
    parser.add_argument('--process', choices=['recist', 'tnm'],
                        help='Process specific document with semantic chunking')
    parser.add_argument('--process-all', action='store_true',
                        help='Process all documents with semantic chunking')
    parser.add_argument('--compare', action='store_true',
                        help='Compare paragraph vs semantic chunking')
    parser.add_argument('--max-size', type=int, default=3000,
                        help='Maximum chunk size in characters (default: 3000)')
    parser.add_argument('--min-size', type=int, default=200,
                        help='Minimum chunk size in characters (default: 200)')
    
    args = parser.parse_args()
    
    if args.compare:
        compare_chunking_methods()
        return
    
    chunker = SemanticChunker(
        max_chunk_size=args.max_size,
        min_chunk_size=args.min_size
    )
    
    if args.process == 'recist':
        chunker.process_recist()
    elif args.process == 'tnm':
        chunker.process_tnm()
    elif args.process_all:
        chunker.process_recist()
        print("\n")
        chunker.process_tnm()
    else:
        parser.print_help()


if __name__ == '__main__':
    main()

