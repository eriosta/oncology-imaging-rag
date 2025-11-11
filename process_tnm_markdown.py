#!/usr/bin/env python3
"""
Process TNM Markdown File into Chunks

Usage:
    python3 process_tnm_markdown.py
"""

from pathlib import Path
from src.processing.markdown_processor import TNMMarkdownProcessor
import json


def main():
    print("="*70)
    print("TNM MARKDOWN PROCESSING")
    print("="*70)
    
    # Define paths
    markdown_file = Path("data/tnm9ed/tnm9ed.md")
    output_dir = Path("output/processed_chunks")
    output_dir.mkdir(parents=True, exist_ok=True)
    
    output_file = output_dir / "tnm_markdown_chunks.jsonl"
    
    # Check input file
    if not markdown_file.exists():
        print(f"\n❌ Markdown file not found: {markdown_file}")
        return
    
    print(f"\nInput: {markdown_file}")
    print(f"Output: {output_file}")
    
    # Process
    try:
        processor = TNMMarkdownProcessor(markdown_file)
        chunks = processor.process()
        processor.save_chunks(output_file)
        
        print(f"\n{'='*70}")
        print(f"✅ SUCCESS!")
        print(f"{'='*70}")
        print(f"\nCreated: {len(chunks):,} chunks")
        print(f"Saved to: {output_file}")
        
        # Show statistics
        chunk_sizes = [c.metadata['char_count'] for c in chunks]
        print(f"\nChunk Statistics:")
        print(f"  Min size: {min(chunk_sizes):,} chars")
        print(f"  Max size: {max(chunk_sizes):,} chars")
        print(f"  Mean size: {sum(chunk_sizes)/len(chunks):.0f} chars")
        
        # Show categories
        categories = {}
        for chunk in chunks:
            cat = chunk.metadata.get('category', 'Unknown')
            categories[cat] = categories.get(cat, 0) + 1
        
        print(f"\nBy Category:")
        for cat, count in sorted(categories.items()):
            print(f"  {cat}: {count} chunks")
        
        # Show sample
        print(f"\n{'='*70}")
        print("SAMPLE CHUNKS (first 2)")
        print(f"{'='*70}")
        
        for i, chunk in enumerate(chunks[:2]):
            print(f"\n--- Chunk {i+1} ---")
            print(f"ID: {chunk.chunk_id}")
            print(f"Category: {chunk.metadata.get('category')}")
            print(f"Station: {chunk.metadata.get('station', 'N/A')}")
            print(f"Size: {chunk.metadata['char_count']} chars")
            print(f"\nText preview (first 200 chars):")
            print(f"{chunk.text[:200]}...")
        
        print(f"\n{'='*70}\n")
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()

