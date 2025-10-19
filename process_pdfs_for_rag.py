#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Process PDF documents (guidelines, papers) into RAG-friendly formats.

Extracts text, intelligently chunks content, and creates structured documents
with metadata (page numbers, sections, etc.) optimized for vector search.
"""

import json
import re
from pathlib import Path
from typing import List, Dict, Tuple

try:
    import fitz  # PyMuPDF
    PYMUPDF_AVAILABLE = True
except ImportError:
    PYMUPDF_AVAILABLE = False
    try:
        import PyPDF2
        PYPDF2_AVAILABLE = True
    except ImportError:
        PYPDF2_AVAILABLE = False

def extract_text_pymupdf(pdf_path: Path) -> List[Dict]:
    """Extract text from PDF using PyMuPDF (best quality)."""
    pages = []
    doc = fitz.open(pdf_path)
    
    for page_num, page in enumerate(doc, start=1):
        text = page.get_text()
        pages.append({
            'page_number': page_num,
            'text': text.strip(),
            'char_count': len(text)
        })
    
    doc.close()
    return pages

def extract_text_pypdf2(pdf_path: Path) -> List[Dict]:
    """Extract text from PDF using PyPDF2 (fallback)."""
    pages = []
    
    with open(pdf_path, 'rb') as f:
        reader = PyPDF2.PdfReader(f)
        
        for page_num, page in enumerate(reader.pages, start=1):
            text = page.extract_text()
            pages.append({
                'page_number': page_num,
                'text': text.strip(),
                'char_count': len(text)
            })
    
    return pages

def extract_text_from_pdf(pdf_path: Path) -> List[Dict]:
    """Extract text from PDF using available library."""
    print(f"  Extracting text from {pdf_path.name}...")
    
    if PYMUPDF_AVAILABLE:
        print(f"    Using PyMuPDF (recommended)")
        pages = extract_text_pymupdf(pdf_path)
    elif PYPDF2_AVAILABLE:
        print(f"    Using PyPDF2 (fallback)")
        pages = extract_text_pypdf2(pdf_path)
    else:
        raise ImportError(
            "No PDF library available. Install with:\n"
            "  pip install pymupdf  (recommended)\n"
            "  or\n"
            "  pip install PyPDF2"
        )
    
    total_chars = sum(p['char_count'] for p in pages)
    print(f"    Extracted {len(pages)} pages, {total_chars:,} characters")
    
    return pages

def detect_sections(pages: List[Dict]) -> List[Dict]:
    """Detect section headers in the text."""
    sections = []
    
    # Common patterns for section headers
    section_patterns = [
        r'^#+\s+(.+)$',  # Markdown-style headers
        r'^(\d+\.(?:\d+\.)*)\s+([A-Z][^\n]+)$',  # Numbered sections (1.2.3 Title)
        r'^([A-Z][A-Z\s]{3,}[A-Z])$',  # ALL CAPS HEADERS
        r'^([A-Z][^.!?]+):$',  # Title with colon
    ]
    
    for page in pages:
        lines = page['text'].split('\n')
        
        for line_num, line in enumerate(lines):
            line = line.strip()
            if not line:
                continue
            
            for pattern in section_patterns:
                match = re.match(pattern, line)
                if match:
                    sections.append({
                        'page': page['page_number'],
                        'line': line_num,
                        'title': line,
                        'level': 1  # Could be refined based on pattern
                    })
                    break
    
    print(f"    Detected {len(sections)} potential sections")
    return sections

def chunk_by_pages(pages: List[Dict], chunk_size: int = 1000, overlap: int = 200) -> List[Dict]:
    """Create overlapping chunks from pages."""
    chunks = []
    chunk_id = 0
    
    for page in pages:
        text = page['text']
        page_num = page['page_number']
        
        # If page is smaller than chunk size, keep it as one chunk
        if len(text) <= chunk_size:
            chunks.append({
                'id': f"chunk_{chunk_id}",
                'text': text,
                'page_start': page_num,
                'page_end': page_num,
                'char_count': len(text)
            })
            chunk_id += 1
            continue
        
        # Split long pages into chunks with overlap
        start = 0
        while start < len(text):
            end = start + chunk_size
            
            # Try to break at sentence boundary
            if end < len(text):
                # Look for sentence ending
                sentence_end = text.rfind('. ', start, end)
                if sentence_end > start + chunk_size // 2:
                    end = sentence_end + 1
            
            chunk_text = text[start:end].strip()
            
            if chunk_text:
                chunks.append({
                    'id': f"chunk_{chunk_id}",
                    'text': chunk_text,
                    'page_start': page_num,
                    'page_end': page_num,
                    'char_count': len(chunk_text)
                })
                chunk_id += 1
            
            # Move start with overlap
            start = end - overlap
            if start >= len(text):
                break
    
    return chunks

def chunk_by_paragraphs(pages: List[Dict], min_chunk_size: int = 500, max_chunk_size: int = 1500) -> List[Dict]:
    """Create chunks by combining paragraphs."""
    chunks = []
    chunk_id = 0
    
    for page in pages:
        text = page['text']
        page_num = page['page_number']
        
        # Split into paragraphs
        paragraphs = [p.strip() for p in text.split('\n\n') if p.strip()]
        
        current_chunk = []
        current_size = 0
        
        for para in paragraphs:
            para_size = len(para)
            
            # If single paragraph exceeds max size, split it
            if para_size > max_chunk_size:
                if current_chunk:
                    chunks.append({
                        'id': f"chunk_{chunk_id}",
                        'text': '\n\n'.join(current_chunk),
                        'page_start': page_num,
                        'page_end': page_num,
                        'char_count': current_size
                    })
                    chunk_id += 1
                    current_chunk = []
                    current_size = 0
                
                # Split large paragraph
                sentences = re.split(r'(?<=[.!?])\s+', para)
                temp_chunk = []
                temp_size = 0
                
                for sent in sentences:
                    if temp_size + len(sent) > max_chunk_size and temp_chunk:
                        chunks.append({
                            'id': f"chunk_{chunk_id}",
                            'text': ' '.join(temp_chunk),
                            'page_start': page_num,
                            'page_end': page_num,
                            'char_count': temp_size
                        })
                        chunk_id += 1
                        temp_chunk = []
                        temp_size = 0
                    
                    temp_chunk.append(sent)
                    temp_size += len(sent) + 1
                
                if temp_chunk:
                    chunks.append({
                        'id': f"chunk_{chunk_id}",
                        'text': ' '.join(temp_chunk),
                        'page_start': page_num,
                        'page_end': page_num,
                        'char_count': temp_size
                    })
                    chunk_id += 1
                
                continue
            
            # Add paragraph to current chunk if it fits
            if current_size + para_size <= max_chunk_size:
                current_chunk.append(para)
                current_size += para_size + 2  # +2 for paragraph separator
            else:
                # Save current chunk if it meets min size
                if current_size >= min_chunk_size:
                    chunks.append({
                        'id': f"chunk_{chunk_id}",
                        'text': '\n\n'.join(current_chunk),
                        'page_start': page_num,
                        'page_end': page_num,
                        'char_count': current_size
                    })
                    chunk_id += 1
                
                # Start new chunk
                current_chunk = [para]
                current_size = para_size
        
        # Save remaining chunk
        if current_chunk and current_size >= min_chunk_size:
            chunks.append({
                'id': f"chunk_{chunk_id}",
                'text': '\n\n'.join(current_chunk),
                'page_start': page_num,
                'page_end': page_num,
                'char_count': current_size
            })
            chunk_id += 1
    
    return chunks

def create_rag_documents(chunks: List[Dict], pdf_name: str, doc_type: str = "guideline") -> List[Dict]:
    """Convert chunks into RAG-optimized documents."""
    documents = []
    
    for chunk in chunks:
        doc = {
            'id': f"{pdf_name}_{chunk['id']}",
            'text': chunk['text'],
            'metadata': {
                'source': pdf_name,
                'type': doc_type,
                'page_start': chunk['page_start'],
                'page_end': chunk['page_end'],
                'char_count': chunk['char_count'],
            }
        }
        documents.append(doc)
    
    return documents

def extract_metadata_from_content(pages: List[Dict]) -> Dict:
    """Try to extract metadata (title, authors, etc.) from content."""
    metadata = {
        'title': None,
        'authors': [],
        'keywords': [],
    }
    
    # Usually title is on first page
    if pages:
        first_page = pages[0]['text']
        lines = [l.strip() for l in first_page.split('\n') if l.strip()]
        
        # First substantial line is often the title
        for line in lines[:10]:
            if len(line) > 20 and not line.startswith('http'):
                metadata['title'] = line
                break
    
    return metadata

def process_pdf(pdf_path: Path, output_dir: Path, chunk_method: str = 'paragraph'):
    """Process a single PDF file."""
    print(f"\nProcessing: {pdf_path.name}")
    
    # Extract text
    pages = extract_text_from_pdf(pdf_path)
    
    # Detect sections (for future use)
    sections = detect_sections(pages)
    
    # Extract metadata
    metadata = extract_metadata_from_content(pages)
    
    # Create chunks
    print(f"  Creating chunks using '{chunk_method}' method...")
    if chunk_method == 'paragraph':
        chunks = chunk_by_paragraphs(pages, min_chunk_size=500, max_chunk_size=1500)
    else:  # 'page'
        chunks = chunk_by_pages(pages, chunk_size=1000, overlap=200)
    
    print(f"    Created {len(chunks)} chunks")
    
    # Calculate statistics
    avg_chunk_size = sum(c['char_count'] for c in chunks) / len(chunks) if chunks else 0
    
    # Create RAG documents
    pdf_basename = pdf_path.stem
    doc_type = 'guideline' if 'RECIST' in pdf_path.name or 'guideline' in pdf_path.name.lower() else 'document'
    rag_docs = create_rag_documents(chunks, pdf_basename, doc_type)
    
    # Save outputs
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Full structured data
    full_data = {
        'source_file': pdf_path.name,
        'metadata': metadata,
        'total_pages': len(pages),
        'total_chunks': len(chunks),
        'avg_chunk_size': round(avg_chunk_size, 1),
        'sections': sections,
        'pages': pages,
        'chunks': chunks,
    }
    
    with open(output_dir / f"{pdf_basename}_full.json", 'w', encoding='utf-8') as f:
        json.dump(full_data, f, indent=2, ensure_ascii=False)
    
    # RAG documents
    with open(output_dir / f"{pdf_basename}_rag_documents.json", 'w', encoding='utf-8') as f:
        json.dump(rag_docs, f, indent=2, ensure_ascii=False)
    
    # Text chunks only (lighter file)
    text_chunks = [{'id': c['id'], 'text': c['text'], 'page': c['page_start']} for c in chunks]
    with open(output_dir / f"{pdf_basename}_text_chunks.json", 'w', encoding='utf-8') as f:
        json.dump(text_chunks, f, indent=2, ensure_ascii=False)
    
    # Statistics
    stats = {
        'source_file': pdf_path.name,
        'total_pages': len(pages),
        'total_chunks': len(chunks),
        'avg_chunk_size': round(avg_chunk_size, 1),
        'sections_detected': len(sections),
        'metadata': metadata,
    }
    
    with open(output_dir / f"{pdf_basename}_statistics.json", 'w', encoding='utf-8') as f:
        json.dump(stats, f, indent=2, ensure_ascii=False)
    
    print(f"\n  ✅ Processed: {len(chunks)} chunks from {len(pages)} pages")
    print(f"     Avg chunk size: {avg_chunk_size:.0f} chars")
    print(f"     Output: {output_dir}/")
    
    return stats

def main():
    """Main processing function."""
    print("="*70)
    print("PDF PROCESSOR FOR RAG")
    print("="*70)
    
    # Check if PDF libraries are available
    if not PYMUPDF_AVAILABLE and not PYPDF2_AVAILABLE:
        print("\n❌ Error: No PDF library found!")
        print("\nPlease install one:")
        print("  pip install pymupdf  (recommended, better quality)")
        print("  pip install PyPDF2   (fallback option)")
        return
    
    # Process PDFs in guidelines directory
    guidelines_dir = Path('data/guidelines')
    output_dir = guidelines_dir / 'processed'
    
    pdf_files = list(guidelines_dir.glob('*.pdf'))
    
    if not pdf_files:
        print(f"\n❌ No PDF files found in {guidelines_dir}")
        return
    
    print(f"\nFound {len(pdf_files)} PDF(s) to process:")
    for pdf in pdf_files:
        print(f"  - {pdf.name}")
    
    all_stats = []
    
    for pdf_path in pdf_files:
        try:
            stats = process_pdf(pdf_path, output_dir, chunk_method='paragraph')
            all_stats.append(stats)
        except Exception as e:
            print(f"\n❌ Error processing {pdf_path.name}: {e}")
            import traceback
            traceback.print_exc()
    
    # Summary
    if all_stats:
        total_chunks = sum(s['total_chunks'] for s in all_stats)
        total_pages = sum(s['total_pages'] for s in all_stats)
        
        print("\n" + "="*70)
        print("PROCESSING COMPLETE")
        print("="*70)
        print(f"\n📊 Processed {len(all_stats)} PDF(s):")
        for s in all_stats:
            print(f"   - {s['source_file']}: {s['total_chunks']} chunks from {s['total_pages']} pages")
        
        print(f"\n📁 Output: {output_dir}/")
        print(f"\n🚀 Ready for RAG ingestion!")
        print(f"   Use: *_rag_documents.json or *_text_chunks.json")

if __name__ == '__main__':
    main()


