#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Process PubMed abstracts into RAG-ready format.

Converts abstracts from JSON into optimized documents for vector databases:
- Combines title + abstract for searchable text
- Preserves metadata (PMID, journal, year, authors)
- Creates text chunks optimized for embeddings
- Generates statistics and summaries

Usage:
    python process_abstracts_for_rag.py
"""

import json
import csv
from pathlib import Path
from collections import defaultdict
from datetime import datetime


def load_abstracts(abstracts_json):
    """Load abstracts from JSON file"""
    with open(abstracts_json, 'r', encoding='utf-8') as f:
        return json.load(f)


def create_rag_documents(abstracts):
    """
    Convert abstracts into RAG-optimized documents.
    
    Each document contains:
    - id: unique identifier (PMID)
    - text: searchable text (title + abstract)
    - metadata: journal, year, authors, etc.
    """
    documents = []
    
    for abstract in abstracts:
        # Combine title and abstract for full searchable text
        title = abstract.get('title', '').strip()
        abstract_text = abstract.get('abstract', '').strip()
        
        # Create searchable text
        searchable_text = f"{title}\n\n{abstract_text}"
        
        # Build metadata
        metadata = {
            'pmid': abstract.get('pmid', ''),
            'journal': abstract.get('journal', ''),
            'year': abstract.get('year', ''),
            'authors': abstract.get('authors', []),
            'source': 'pubmed',
            'document_type': 'abstract'
        }
        
        doc = {
            'id': f"PMID:{abstract.get('pmid', '')}",
            'title': title,
            'text': searchable_text,
            'metadata': metadata
        }
        
        documents.append(doc)
    
    return documents


def create_text_chunks(abstracts, max_chunk_length=1000):
    """
    Create text chunks optimized for embedding models.
    
    For abstracts, each abstract is typically one chunk since they're
    already concise. For longer abstracts, split by sentences.
    """
    chunks = []
    
    for abstract in abstracts:
        pmid = abstract.get('pmid', '')
        title = abstract.get('title', '').strip()
        abstract_text = abstract.get('abstract', '').strip()
        
        # Full text
        full_text = f"{title}\n\n{abstract_text}"
        
        # If text is short enough, keep as single chunk
        if len(full_text) <= max_chunk_length:
            chunks.append({
                'chunk_id': f"PMID:{pmid}_chunk_0",
                'pmid': pmid,
                'text': full_text,
                'chunk_index': 0,
                'total_chunks': 1
            })
        else:
            # Split longer abstracts by sentences
            sentences = abstract_text.replace('. ', '.|').split('|')
            
            current_chunk = title + "\n\n"
            chunk_index = 0
            
            for sentence in sentences:
                sentence = sentence.strip()
                if not sentence:
                    continue
                
                # If adding this sentence exceeds limit, save current chunk
                if len(current_chunk) + len(sentence) > max_chunk_length and len(current_chunk) > len(title) + 2:
                    chunks.append({
                        'chunk_id': f"PMID:{pmid}_chunk_{chunk_index}",
                        'pmid': pmid,
                        'text': current_chunk.strip(),
                        'chunk_index': chunk_index,
                        'total_chunks': 'TBD'  # Will update later
                    })
                    current_chunk = title + "\n\n" + sentence + ". "
                    chunk_index += 1
                else:
                    current_chunk += sentence + ". "
            
            # Save remaining chunk
            if current_chunk.strip():
                chunks.append({
                    'chunk_id': f"PMID:{pmid}_chunk_{chunk_index}",
                    'pmid': pmid,
                    'text': current_chunk.strip(),
                    'chunk_index': chunk_index,
                    'total_chunks': chunk_index + 1
                })
            
            # Update total_chunks for all chunks from this abstract
            total = chunk_index + 1
            for chunk in chunks:
                if chunk['pmid'] == pmid and chunk['total_chunks'] == 'TBD':
                    chunk['total_chunks'] = total
    
    return chunks


def generate_statistics(abstracts, documents, chunks):
    """Generate statistics about the processed data"""
    
    # Basic counts
    stats = {
        'total_abstracts': len(abstracts),
        'total_documents': len(documents),
        'total_chunks': len(chunks),
        'processing_date': datetime.now().isoformat()
    }
    
    # Journal distribution
    journal_counts = defaultdict(int)
    for abstract in abstracts:
        journal = abstract.get('journal', 'Unknown')
        journal_counts[journal] += 1
    
    stats['journal_distribution'] = dict(sorted(
        journal_counts.items(),
        key=lambda x: x[1],
        reverse=True
    ))
    
    # Year distribution
    year_counts = defaultdict(int)
    for abstract in abstracts:
        year = abstract.get('year', 'Unknown')
        year_counts[year] += 1
    
    stats['year_distribution'] = dict(sorted(year_counts.items()))
    
    # Text length statistics
    abstract_lengths = [len(a.get('abstract', '')) for a in abstracts]
    title_lengths = [len(a.get('title', '')) for a in abstracts]
    chunk_lengths = [len(c['text']) for c in chunks]
    
    stats['text_length_stats'] = {
        'abstract': {
            'min': min(abstract_lengths) if abstract_lengths else 0,
            'max': max(abstract_lengths) if abstract_lengths else 0,
            'avg': sum(abstract_lengths) / len(abstract_lengths) if abstract_lengths else 0
        },
        'title': {
            'min': min(title_lengths) if title_lengths else 0,
            'max': max(title_lengths) if title_lengths else 0,
            'avg': sum(title_lengths) / len(title_lengths) if title_lengths else 0
        },
        'chunk': {
            'min': min(chunk_lengths) if chunk_lengths else 0,
            'max': max(chunk_lengths) if chunk_lengths else 0,
            'avg': sum(chunk_lengths) / len(chunk_lengths) if chunk_lengths else 0
        }
    }
    
    return stats


def save_outputs(output_dir, documents, chunks, stats, abstracts):
    """Save all processed outputs"""
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Save RAG documents
    documents_file = output_dir / "abstracts_rag_documents.json"
    with open(documents_file, 'w', encoding='utf-8') as f:
        json.dump(documents, f, indent=2, ensure_ascii=False)
    print(f"  Saved: {documents_file.name} ({len(documents):,} documents)")
    
    # Save text chunks
    chunks_file = output_dir / "abstracts_text_chunks.json"
    with open(chunks_file, 'w', encoding='utf-8') as f:
        json.dump(chunks, f, indent=2, ensure_ascii=False)
    print(f"  Saved: {chunks_file.name} ({len(chunks):,} chunks)")
    
    # Save statistics
    stats_file = output_dir / "abstracts_statistics.json"
    with open(stats_file, 'w', encoding='utf-8') as f:
        json.dump(stats, f, indent=2, ensure_ascii=False)
    print(f"  Saved: {stats_file.name}")
    
    # Save simple CSV for viewing
    csv_file = output_dir / "abstracts_for_rag.csv"
    with open(csv_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=['pmid', 'title', 'journal', 'year', 'abstract', 'text_length'])
        writer.writeheader()
        for abstract in abstracts:
            writer.writerow({
                'pmid': abstract.get('pmid', ''),
                'title': abstract.get('title', ''),
                'journal': abstract.get('journal', ''),
                'year': abstract.get('year', ''),
                'abstract': abstract.get('abstract', ''),
                'text_length': len(abstract.get('abstract', ''))
            })
    print(f"  Saved: {csv_file.name}")


def process_category(category_dir, output_base_dir):
    """Process a single category of abstracts"""
    category_name = category_dir.name
    abstracts_json = category_dir / "abstracts.json"
    
    if not abstracts_json.exists():
        print(f"  ⚠️  No abstracts.json found in {category_name}")
        return None
    
    print(f"\n[{category_name}]")
    
    # Load abstracts
    abstracts = load_abstracts(abstracts_json)
    print(f"  Loaded: {len(abstracts):,} abstracts")
    
    # Create RAG documents
    documents = create_rag_documents(abstracts)
    print(f"  Created: {len(documents):,} RAG documents")
    
    # Create text chunks
    chunks = create_text_chunks(abstracts, max_chunk_length=1000)
    print(f"  Created: {len(chunks):,} text chunks")
    
    # Generate statistics
    stats = generate_statistics(abstracts, documents, chunks)
    
    # Save outputs
    output_dir = output_base_dir / category_name
    save_outputs(output_dir, documents, chunks, stats, abstracts)
    
    return {
        'category': category_name,
        'abstracts': len(abstracts),
        'documents': len(documents),
        'chunks': len(chunks)
    }


def process_all_abstracts(data_dir):
    """Process all abstract categories"""
    abstracts_dir = data_dir / "pubmed_abstracts"
    output_base_dir = abstracts_dir / "processed"
    
    if not abstracts_dir.exists():
        print(f"Error: {abstracts_dir} not found")
        return
    
    print("=" * 70)
    print("PROCESSING PUBMED ABSTRACTS FOR RAG")
    print("=" * 70)
    
    # Process individual categories
    categories = [d for d in abstracts_dir.iterdir() 
                  if d.is_dir() and d.name not in ['processed', 'custom_query']]
    
    summaries = []
    for category_dir in sorted(categories):
        result = process_category(category_dir, output_base_dir)
        if result:
            summaries.append(result)
    
    # Process combined file
    all_abstracts_json = abstracts_dir / "all_abstracts.json"
    if all_abstracts_json.exists():
        print(f"\n[ALL_COMBINED]")
        abstracts = load_abstracts(all_abstracts_json)
        print(f"  Loaded: {len(abstracts):,} total abstracts")
        
        documents = create_rag_documents(abstracts)
        chunks = create_text_chunks(abstracts, max_chunk_length=1000)
        stats = generate_statistics(abstracts, documents, chunks)
        
        output_dir = output_base_dir / "all_combined"
        save_outputs(output_dir, documents, chunks, stats, abstracts)
        
        summaries.append({
            'category': 'ALL_COMBINED',
            'abstracts': len(abstracts),
            'documents': len(documents),
            'chunks': len(chunks)
        })
    
    # Print summary
    print("\n" + "=" * 70)
    print("PROCESSING COMPLETE")
    print("=" * 70)
    print(f"\nProcessed {len(summaries)} categories:\n")
    
    for summary in summaries:
        print(f"  {summary['category']:30s}: {summary['abstracts']:5,} abstracts → "
              f"{summary['documents']:5,} docs → {summary['chunks']:5,} chunks")
    
    print(f"\n✅ All processed files saved to: {output_base_dir}/")
    print("\nRAG-ready outputs:")
    print("  - *_rag_documents.json : Full documents with metadata (for vector DB)")
    print("  - *_text_chunks.json   : Text chunks optimized for embeddings")
    print("  - *_statistics.json    : Processing statistics and distributions")
    print("  - *_for_rag.csv        : Human-readable CSV for review")


def main():
    # Set data directory
    data_dir = Path(__file__).parent / "data"
    
    # Process all abstracts
    process_all_abstracts(data_dir)


if __name__ == '__main__':
    main()


