#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Quality Assurance / Quality Improvement Script for RAG Data

Analyzes processed RAG documents to identify quality issues and clustering problems.

Usage:
    python qa_rag_data.py --analyze-all
    python qa_rag_data.py --source recist
    python qa_rag_data.py --compare loinc recist
"""

import json
import argparse
from pathlib import Path
from collections import defaultdict, Counter
import statistics


class RAGDataQA:
    """Quality analysis for RAG documents"""
    
    def __init__(self):
        self.data_dir = Path("data")
        self.sources = {
            'radlex': self.data_dir / "radlex/processed/radlex_rag_documents.json",
            'loinc': self.data_dir / "loinc/processed/loinc_rag_documents.json",
            'recist': self.data_dir / "guidelines/processed/RECIST_1.1_EORTC_rag_documents.json",
            'tnm': self.data_dir / "tnm9ed/processed/Staging Cards 9th Edition 2024_rag_documents.json",
            'pubmed': self.data_dir / "pubmed_abstracts/processed/pubmed_abstracts_rag_documents.json",
        }
    
    def load_documents(self, source: str):
        """Load documents from a source"""
        if source not in self.sources:
            raise ValueError(f"Unknown source: {source}. Available: {list(self.sources.keys())}")
        
        file_path = self.sources[source]
        if not file_path.exists():
            print(f"⚠️  File not found: {file_path}")
            return []
        
        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    
    def analyze_source(self, source: str):
        """Comprehensive analysis of a data source"""
        print(f"\n{'='*70}")
        print(f"ANALYZING: {source.upper()}")
        print('='*70)
        
        docs = self.load_documents(source)
        
        if not docs:
            print("❌ No documents found")
            return None
        
        print(f"\n📊 Basic Statistics:")
        print(f"   Total documents: {len(docs):,}")
        
        # Text length analysis
        text_lengths = [len(doc.get('text', '')) for doc in docs]
        print(f"\n📏 Text Length Analysis:")
        print(f"   Min length: {min(text_lengths):,} chars")
        print(f"   Max length: {max(text_lengths):,} chars")
        print(f"   Mean length: {statistics.mean(text_lengths):.1f} chars")
        print(f"   Median length: {statistics.median(text_lengths):.1f} chars")
        print(f"   Std dev: {statistics.stdev(text_lengths):.1f} chars" if len(text_lengths) > 1 else "   N/A")
        
        # Length distribution
        length_buckets = {
            'Very short (<100)': sum(1 for l in text_lengths if l < 100),
            'Short (100-500)': sum(1 for l in text_lengths if 100 <= l < 500),
            'Medium (500-1500)': sum(1 for l in text_lengths if 500 <= l < 1500),
            'Long (1500-3000)': sum(1 for l in text_lengths if 1500 <= l < 3000),
            'Very long (>3000)': sum(1 for l in text_lengths if l >= 3000),
        }
        print(f"\n   Length Distribution:")
        for bucket, count in length_buckets.items():
            pct = (count / len(docs)) * 100
            print(f"      {bucket:20s}: {count:6,} ({pct:5.1f}%)")
        
        # Content diversity analysis
        print(f"\n🔤 Content Diversity:")
        
        # Unique words
        all_words = []
        for doc in docs[:1000]:  # Sample first 1000 for performance
            words = doc.get('text', '').lower().split()
            all_words.extend(words)
        
        if all_words:
            unique_words = len(set(all_words))
            total_words = len(all_words)
            print(f"   Sample size: {min(len(docs), 1000):,} documents")
            print(f"   Total words: {total_words:,}")
            print(f"   Unique words: {unique_words:,}")
            print(f"   Vocabulary diversity: {(unique_words/total_words)*100:.1f}%")
        
        # Metadata analysis
        if docs and 'metadata' in docs[0]:
            print(f"\n📋 Metadata Analysis:")
            metadata_keys = set()
            for doc in docs[:100]:
                if 'metadata' in doc:
                    metadata_keys.update(doc['metadata'].keys())
            print(f"   Metadata fields: {', '.join(sorted(metadata_keys))}")
            
            # Type distribution
            if 'type' in metadata_keys:
                types = [doc.get('metadata', {}).get('type') for doc in docs]
                type_counts = Counter(types)
                print(f"\n   Document Types:")
                for dtype, count in type_counts.most_common():
                    print(f"      {dtype:20s}: {count:6,}")
        
        # Clustering potential analysis
        print(f"\n🎯 Clustering Potential:")
        
        # Calculate text similarity indicators
        first_words = Counter()
        for doc in docs[:1000]:
            text = doc.get('text', '').strip()
            if text:
                first_word = text.split()[0] if text.split() else ''
                first_words[first_word] += 1
        
        if first_words:
            top_starts = first_words.most_common(10)
            print(f"   Top 10 starting words (indicates structure):")
            for word, count in top_starts:
                pct = (count / min(len(docs), 1000)) * 100
                print(f"      '{word}': {count} times ({pct:.1f}%)")
        
        # Repetition analysis (good for clustering)
        print(f"\n   Repetition Analysis:")
        common_phrases = self._find_common_phrases(docs[:500])
        if common_phrases:
            print(f"      Found {len(common_phrases)} common 3-word phrases")
            print(f"      Top 5:")
            for phrase, count in common_phrases[:5]:
                print(f"         '{phrase}': {count} times")
        
        # Quality issues
        print(f"\n⚠️  Potential Quality Issues:")
        issues = []
        
        # Check for very short texts
        very_short = sum(1 for l in text_lengths if l < 50)
        if very_short > 0:
            issues.append(f"   - {very_short} documents with <50 chars (may lack content)")
        
        # Check for very long texts
        very_long = sum(1 for l in text_lengths if l > 5000)
        if very_long > 0:
            issues.append(f"   - {very_long} documents with >5000 chars (may need chunking)")
        
        # Check for high length variance
        if len(text_lengths) > 1 and statistics.stdev(text_lengths) > statistics.mean(text_lengths):
            issues.append(f"   - High length variance (stdev > mean) - inconsistent chunking")
        
        # Check for low diversity
        if all_words and unique_words / total_words < 0.3:
            issues.append(f"   - Low vocabulary diversity (<30%) - repetitive content")
        
        if not issues:
            print(f"   ✅ No major quality issues detected")
        else:
            for issue in issues:
                print(issue)
        
        return {
            'source': source,
            'count': len(docs),
            'text_lengths': text_lengths,
            'mean_length': statistics.mean(text_lengths),
            'std_length': statistics.stdev(text_lengths) if len(text_lengths) > 1 else 0,
        }
    
    def _find_common_phrases(self, docs, top_n=20):
        """Find common 3-word phrases"""
        phrase_counter = Counter()
        
        for doc in docs:
            words = doc.get('text', '').lower().split()
            for i in range(len(words) - 2):
                phrase = ' '.join(words[i:i+3])
                phrase_counter[phrase] += 1
        
        return [(p, c) for p, c in phrase_counter.most_common(top_n) if c > 2]
    
    def compare_sources(self, source1: str, source2: str):
        """Compare two sources to understand clustering differences"""
        print(f"\n{'='*70}")
        print(f"COMPARING: {source1.upper()} vs {source2.upper()}")
        print('='*70)
        
        stats1 = self.analyze_source(source1)
        stats2 = self.analyze_source(source2)
        
        if not stats1 or not stats2:
            return
        
        print(f"\n{'='*70}")
        print("COMPARISON SUMMARY")
        print('='*70)
        
        print(f"\n📊 Dataset Size:")
        print(f"   {source1:15s}: {stats1['count']:,} documents")
        print(f"   {source2:15s}: {stats2['count']:,} documents")
        print(f"   Ratio: {stats1['count']/stats2['count']:.1f}x")
        
        print(f"\n📏 Average Length:")
        print(f"   {source1:15s}: {stats1['mean_length']:.1f} chars")
        print(f"   {source2:15s}: {stats2['mean_length']:.1f} chars")
        
        print(f"\n📐 Consistency (lower std = better clustering):")
        print(f"   {source1:15s}: std={stats1['std_length']:.1f}")
        print(f"   {source2:15s}: std={stats2['std_length']:.1f}")
        
        print(f"\n💡 Clustering Insights:")
        
        # Large dataset advantage
        if stats1['count'] > stats2['count'] * 10:
            print(f"   • {source1} has {stats1['count']/stats2['count']:.0f}x more documents")
            print(f"     → More data = more/tighter clusters")
        elif stats2['count'] > stats1['count'] * 10:
            print(f"   • {source2} has {stats2['count']/stats1['count']:.0f}x more documents")
            print(f"     → More data = more/tighter clusters")
        
        # Consistency advantage
        coef_var1 = stats1['std_length'] / stats1['mean_length']
        coef_var2 = stats2['std_length'] / stats2['mean_length']
        
        print(f"\n   Coefficient of Variation (lower = more consistent):")
        print(f"   • {source1}: {coef_var1:.2f}")
        print(f"   • {source2}: {coef_var2:.2f}")
        
        if coef_var1 < coef_var2 * 0.7:
            print(f"   → {source1} is more consistent = better clustering")
        elif coef_var2 < coef_var1 * 0.7:
            print(f"   → {source2} is more consistent = better clustering")
    
    def suggest_improvements(self, source: str):
        """Suggest improvements for clustering"""
        print(f"\n{'='*70}")
        print(f"IMPROVEMENT SUGGESTIONS FOR: {source.upper()}")
        print('='*70)
        
        docs = self.load_documents(source)
        if not docs:
            return
        
        text_lengths = [len(doc.get('text', '')) for doc in docs]
        mean_len = statistics.mean(text_lengths)
        std_len = statistics.stdev(text_lengths) if len(text_lengths) > 1 else 0
        
        suggestions = []
        
        # Small dataset
        if len(docs) < 100:
            suggestions.append({
                'issue': f"Small dataset ({len(docs)} documents)",
                'impact': 'HIGH',
                'suggestion': 'Small datasets naturally form fewer/looser clusters. Consider:',
                'actions': [
                    '- This may be expected behavior (guidelines are naturally small)',
                    '- Combine with related documents for analysis',
                    '- Use lower min_cluster_size in clustering algorithms',
                    '- Focus on semantic search rather than clustering'
                ]
            })
        
        # High variance
        if std_len > mean_len:
            suggestions.append({
                'issue': f"High length variance (std={std_len:.0f}, mean={mean_len:.0f})",
                'impact': 'MEDIUM',
                'suggestion': 'Inconsistent chunk sizes hurt clustering',
                'actions': [
                    '- Re-chunk with more consistent target size',
                    '- Use semantic chunking (by section/topic)',
                    '- Consider splitting very long chunks',
                    '- Normalize embeddings by length'
                ]
            })
        
        # Very small chunks
        very_short = sum(1 for l in text_lengths if l < 100)
        if very_short > len(docs) * 0.1:
            suggestions.append({
                'issue': f"{very_short} chunks < 100 characters",
                'impact': 'HIGH',
                'suggestion': 'Very short chunks lack semantic content',
                'actions': [
                    '- Merge short consecutive chunks',
                    '- Add more context (section headers, etc)',
                    '- Filter out chunks < 50 chars',
                    '- Review chunking strategy'
                ]
            })
        
        # Content diversity
        sample_texts = [doc.get('text', '')[:100] for doc in docs[:20]]
        if len(set(sample_texts)) < len(sample_texts) * 0.5:
            suggestions.append({
                'issue': 'Low content diversity detected',
                'impact': 'MEDIUM',
                'suggestion': 'Repetitive content may collapse into few clusters',
                'actions': [
                    '- This may be expected (structured data)',
                    '- Use TF-IDF weighting for embeddings',
                    '- Add more context to distinguish chunks',
                    '- Consider metadata-based clustering'
                ]
            })
        
        if not suggestions:
            print("\n✅ No major issues detected!")
            print("\nData appears well-suited for clustering.")
        else:
            for i, sug in enumerate(suggestions, 1):
                print(f"\n{i}. [{sug['impact']}] {sug['issue']}")
                print(f"   {sug['suggestion']}")
                for action in sug['actions']:
                    print(f"      {action}")
        
        # General recommendations
        print(f"\n💡 General Recommendations for {source}:")
        
        if source == 'recist':
            print("\n   RECIST is a small, heterogeneous guideline document:")
            print("   • 79 chunks covering different measurement rules")
            print("   • Each section discusses different aspects (target lesions, response, etc)")
            print("   • NOT expected to form tight clusters like structured data")
            print("\n   ✅ This is NORMAL behavior - guidelines are topically diverse")
            print("   ✅ Focus on semantic search accuracy, not clustering")
            print("   ✅ Use metadata (page, section) for organization")
        
        elif source == 'loinc':
            print("\n   LOINC clusters well because:")
            print("   • 6,973 similar procedure descriptions")
            print("   • 190K+ synonyms create semantic relationships")
            print("   • Structured, consistent format")
            print("   • Natural groupings (by modality, anatomy, etc)")
        
        elif source == 'tnm':
            print("\n   TNM is small but structured:")
            print("   • 44 staging cards with consistent format")
            print("   • Should form some clusters by cancer type")
            print("   • May benefit from adding section headers to chunks")
    
    def analyze_all(self):
        """Analyze all available sources"""
        print("\n" + "="*70)
        print("ANALYZING ALL DATA SOURCES")
        print("="*70)
        
        results = {}
        for source in self.sources.keys():
            try:
                result = self.analyze_source(source)
                if result:
                    results[source] = result
            except Exception as e:
                print(f"\n❌ Error analyzing {source}: {e}")
        
        # Summary
        print("\n" + "="*70)
        print("SUMMARY - CLUSTERING POTENTIAL RANKING")
        print("="*70)
        
        # Rank by clustering potential (size * consistency)
        rankings = []
        for source, stats in results.items():
            coef_var = stats['std_length'] / stats['mean_length'] if stats['mean_length'] > 0 else 999
            # Lower coef_var = better consistency
            # Higher count = more clusters
            score = stats['count'] / (1 + coef_var)
            rankings.append((source, score, stats['count']))
        
        rankings.sort(key=lambda x: x[1], reverse=True)
        
        print("\n   Rank | Source       | Documents  | Clustering Potential")
        print("   " + "-"*60)
        for i, (source, score, count) in enumerate(rankings, 1):
            bar = "█" * min(50, int(score / 100))
            print(f"   {i:4d} | {source:12s} | {count:10,} | {bar}")


def main():
    parser = argparse.ArgumentParser(
        description="Quality Assurance for RAG Data",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Analyze all sources
  python qa_rag_data.py --analyze-all
  
  # Analyze specific source
  python qa_rag_data.py --source recist
  
  # Compare two sources
  python qa_rag_data.py --compare loinc recist
  
  # Get improvement suggestions
  python qa_rag_data.py --improve recist
        """
    )
    
    parser.add_argument('--analyze-all', action='store_true',
                        help='Analyze all data sources')
    parser.add_argument('--source', type=str,
                        help='Analyze specific source (radlex, loinc, recist, tnm, pubmed)')
    parser.add_argument('--compare', nargs=2, metavar=('SOURCE1', 'SOURCE2'),
                        help='Compare two sources')
    parser.add_argument('--improve', type=str,
                        help='Get improvement suggestions for source')
    
    args = parser.parse_args()
    
    qa = RAGDataQA()
    
    if args.analyze_all:
        qa.analyze_all()
    elif args.source:
        qa.analyze_source(args.source)
        qa.suggest_improvements(args.source)
    elif args.compare:
        qa.compare_sources(args.compare[0], args.compare[1])
    elif args.improve:
        qa.suggest_improvements(args.improve)
    else:
        parser.print_help()


if __name__ == '__main__':
    main()


