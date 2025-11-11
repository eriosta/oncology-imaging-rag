"""
Tool to inspect and validate processed chunks.
"""

import json
from pathlib import Path
from collections import Counter
import statistics


def inspect_chunks(chunk_file: Path):
    """Load and analyze chunks from JSONL file"""
    chunks = []
    
    try:
        with open(chunk_file, 'r', encoding='utf-8') as f:
            for line in f:
                if line.strip():
                    chunks.append(json.loads(line))
    except FileNotFoundError:
        print(f"\n❌ File not found: {chunk_file}")
        return
    except json.JSONDecodeError as e:
        print(f"\n❌ JSON decode error in {chunk_file}: {e}")
        return
    
    if not chunks:
        print(f"\n⚠️  No chunks found in: {chunk_file}")
        return
    
    print(f"\n{'='*70}")
    print(f"INSPECTING: {chunk_file.name}")
    print(f"{'='*70}")
    print(f"Total chunks: {len(chunks):,}")
    
    # Analyze source types
    source_types = Counter(c.get('source_type', 'unknown') for c in chunks)
    print(f"\nSource types:")
    for stype, count in source_types.items():
        print(f"  {stype}: {count:,}")
    
    # Analyze metadata fields
    metadata_keys = set()
    for c in chunks:
        if 'metadata' in c:
            metadata_keys.update(c['metadata'].keys())
    print(f"\nMetadata fields ({len(metadata_keys)}):")
    for key in sorted(metadata_keys):
        print(f"  • {key}")
    
    # Text length statistics
    text_lengths = [len(c.get('text', '')) for c in chunks]
    print(f"\nText length statistics:")
    print(f"  Min: {min(text_lengths):,} chars")
    print(f"  Max: {max(text_lengths):,} chars")
    print(f"  Mean: {statistics.mean(text_lengths):.1f} chars")
    print(f"  Median: {statistics.median(text_lengths):.1f} chars")
    if len(text_lengths) > 1:
        print(f"  Std dev: {statistics.stdev(text_lengths):.1f} chars")
    
    # Length distribution
    buckets = {
        'Very short (<100)': sum(1 for l in text_lengths if l < 100),
        'Short (100-500)': sum(1 for l in text_lengths if 100 <= l < 500),
        'Medium (500-1500)': sum(1 for l in text_lengths if 500 <= l < 1500),
        'Long (1500-3000)': sum(1 for l in text_lengths if 1500 <= l < 3000),
        'Very long (>3000)': sum(1 for l in text_lengths if l >= 3000),
    }
    
    print(f"\nLength distribution:")
    for bucket, count in buckets.items():
        pct = (count / len(chunks)) * 100 if chunks else 0
        bar = '█' * int(pct / 2)
        print(f"  {bucket:20s}: {count:6,} ({pct:5.1f}%) {bar}")
    
    # Show sample chunks
    print(f"\n{'='*70}")
    print("SAMPLE CHUNKS (first 3)")
    print(f"{'='*70}")
    
    for i, chunk in enumerate(chunks[:3]):
        print(f"\n--- Chunk {i+1} ---")
        print(f"ID: {chunk.get('chunk_id', 'N/A')}")
        print(f"Source: {chunk.get('source_type', 'N/A')}")
        print(f"Text length: {len(chunk.get('text', '')):,} chars")
        
        # Show metadata
        if 'metadata' in chunk:
            print(f"Metadata:")
            for key, value in list(chunk['metadata'].items())[:5]:
                if isinstance(value, list) and len(value) > 3:
                    print(f"  {key}: [{len(value)} items]")
                else:
                    value_str = str(value)
                    if len(value_str) > 60:
                        value_str = value_str[:60] + '...'
                    print(f"  {key}: {value_str}")
        
        # Show text preview
        text = chunk.get('text', '')
        text_lines = text.split('\n')
        preview_lines = text_lines[:5]
        print(f"\nText preview:")
        for line in preview_lines:
            if line.strip():
                print(f"  {line[:70]}")
        if len(text_lines) > 5:
            print(f"  ... ({len(text_lines) - 5} more lines)")


def main():
    output_dir = Path("output/processed_chunks").resolve()
    
    print("\n" + "="*70)
    print("CHUNK INSPECTION TOOL")
    print("="*70)
    print(f"\nScanning: {output_dir}")
    
    jsonl_files = list(output_dir.glob("*.jsonl"))
    
    if not jsonl_files:
        print(f"\n⚠️  No JSONL files found in: {output_dir}")
        print("   Run the main pipeline first: python src/main.py")
        return
    
    print(f"Found {len(jsonl_files)} chunk files:\n")
    for f in jsonl_files:
        print(f"  • {f.name}")
    
    # Inspect each file
    for chunk_file in sorted(jsonl_files):
        inspect_chunks(chunk_file)
    
    # Overall summary
    print(f"\n{'='*70}")
    print("OVERALL SUMMARY")
    print(f"{'='*70}")
    
    total_chunks = 0
    total_chars = 0
    
    for chunk_file in jsonl_files:
        try:
            with open(chunk_file, 'r', encoding='utf-8') as f:
                chunks = [json.loads(line) for line in f if line.strip()]
                count = len(chunks)
                chars = sum(len(c.get('text', '')) for c in chunks)
                total_chunks += count
                total_chars += chars
                print(f"  {chunk_file.stem:30s}: {count:6,} chunks ({chars:12,} chars)")
        except Exception as e:
            print(f"  {chunk_file.stem:30s}: ❌ Error: {e}")
    
    print(f"\n  {'TOTAL':30s}: {total_chunks:6,} chunks ({total_chars:12,} chars)")
    print(f"\n{'='*70}\n")


if __name__ == "__main__":
    main()

