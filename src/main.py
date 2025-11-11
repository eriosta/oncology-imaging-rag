"""
Main processing pipeline.
Orchestrates all processors and generates final output.
"""

from pathlib import Path
from processing.radlex_processor import RadLexProcessor
from processing.loinc_processor import LOINCProcessor
from processing.pdf_processor import TNMProcessor, RECISTProcessor
import json
from tqdm import tqdm


def main():
    # Define paths
    data_dir = Path("data").resolve()
    output_dir = Path("output/processed_chunks").resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    
    print("=" * 70)
    print("RAG DATA PROCESSING PIPELINE")
    print("=" * 70)
    print(f"\nData directory: {data_dir}")
    print(f"Output directory: {output_dir}")
    
    # Track results
    results = {}
    
    # ========================================================================
    # [1/4] Process RadLex
    # ========================================================================
    print("\n" + "=" * 70)
    print("[1/4] PROCESSING RADLEX ONTOLOGY")
    print("=" * 70)
    
    try:
        radlex_dir = data_dir / "radlex" / "extracted"
        if not radlex_dir.exists():
            print(f"⚠️  RadLex extracted directory not found: {radlex_dir}")
            print("   Skipping RadLex processing")
            results['radlex'] = {'status': 'skipped', 'chunks': 0}
        else:
            radlex_processor = RadLexProcessor(radlex_dir)
            radlex_chunks = radlex_processor.process()
            radlex_processor.save_chunks(output_dir / "radlex_chunks.jsonl")
            print(f"\n✅ RadLex: {len(radlex_chunks):,} chunks created")
            results['radlex'] = {'status': 'success', 'chunks': len(radlex_chunks)}
    except Exception as e:
        print(f"\n❌ RadLex processing failed: {e}")
        results['radlex'] = {'status': 'failed', 'error': str(e), 'chunks': 0}
    
    # ========================================================================
    # [2/4] Process LOINC
    # ========================================================================
    print("\n" + "=" * 70)
    print("[2/4] PROCESSING LOINC RADIOLOGY PLAYBOOK")
    print("=" * 70)
    
    try:
        loinc_dir = data_dir / "loinc"
        loinc_processor = LOINCProcessor(loinc_dir)
        loinc_chunks = loinc_processor.process()
        loinc_processor.save_chunks(output_dir / "loinc_chunks.jsonl")
        print(f"\n✅ LOINC: {len(loinc_chunks):,} chunks created")
        results['loinc'] = {'status': 'success', 'chunks': len(loinc_chunks)}
    except Exception as e:
        print(f"\n❌ LOINC processing failed: {e}")
        results['loinc'] = {'status': 'failed', 'error': str(e), 'chunks': 0}
    
    # ========================================================================
    # [3/4] Process TNM 9th Edition - Lung Cancer Protocol
    # ========================================================================
    print("\n" + "=" * 70)
    print("[3/4] PROCESSING TNM 9TH EDITION - LUNG CANCER PROTOCOL")
    print("=" * 70)
    
    try:
        # Use the new Lung Protocol PDF with staging criteria
        tnm_pdf = data_dir / "tnm9ed" / "Lung_ Protocol for Cancer Staging Documentation.pdf"
        
        if not tnm_pdf.exists():
            print(f"⚠️  Lung Protocol PDF not found: {tnm_pdf}")
            print("   Skipping TNM processing")
            results['tnm'] = {'status': 'skipped', 'chunks': 0}
        else:
            print(f"   Processing: {tnm_pdf.name}")
            tnm_processor = TNMProcessor(tnm_pdf)
            tnm_chunks = tnm_processor.process()
            tnm_processor.save_chunks(output_dir / "tnm_chunks.jsonl")
            print(f"\n✅ TNM Lung Cancer: {len(tnm_chunks):,} chunks created")
            
            # Collect detailed statistics
            categories = {}
            sections = set()
            chunk_sizes = []
            
            for chunk in tnm_chunks:
                cat = chunk.metadata.get('category', 'Unknown')
                categories[cat] = categories.get(cat, 0) + 1
                
                section = chunk.metadata.get('section')
                if section:
                    sections.add(section)
                
                chunk_sizes.append(chunk.metadata.get('char_count', 0))
            
            results['tnm'] = {
                'status': 'success',
                'chunks': len(tnm_chunks),
                'details': {
                    'source_file': tnm_pdf.name,
                    'cancer_type': 'Lung',
                    'protocol_type': 'staging_documentation',
                    'tnm_edition': '9th',
                    'categories': categories,
                    'major_sections': len(sections),
                    'chunk_statistics': {
                        'min_size': min(chunk_sizes) if chunk_sizes else 0,
                        'max_size': max(chunk_sizes) if chunk_sizes else 0,
                        'mean_size': round(sum(chunk_sizes) / len(chunk_sizes), 1) if chunk_sizes else 0,
                        'total_chars': sum(chunk_sizes)
                    }
                }
            }
    except Exception as e:
        print(f"\n❌ TNM processing failed: {e}")
        import traceback
        traceback.print_exc()
        results['tnm'] = {'status': 'failed', 'error': str(e), 'chunks': 0}
    
    # ========================================================================
    # [4/4] Process RECIST Guidelines
    # ========================================================================
    print("\n" + "=" * 70)
    print("[4/4] PROCESSING RECIST 1.1 GUIDELINES")
    print("=" * 70)
    
    try:
        recist_pdf_files = list((data_dir / "guidelines").glob("RECIST*.pdf"))
        if not recist_pdf_files:
            print(f"⚠️  No RECIST PDF found in: {data_dir / 'guidelines'}")
            print("   Skipping RECIST processing")
            results['recist'] = {'status': 'skipped', 'chunks': 0}
        else:
            recist_pdf = recist_pdf_files[0]
            print(f"   Processing: {recist_pdf.name}")
            recist_processor = RECISTProcessor(recist_pdf)
            recist_chunks = recist_processor.process()
            recist_processor.save_chunks(output_dir / "recist_chunks.jsonl")
            print(f"\n✅ RECIST: {len(recist_chunks):,} chunks created")
            results['recist'] = {'status': 'success', 'chunks': len(recist_chunks)}
    except Exception as e:
        print(f"\n❌ RECIST processing failed: {e}")
        import traceback
        traceback.print_exc()
        results['recist'] = {'status': 'failed', 'error': str(e), 'chunks': 0}
    
    # ========================================================================
    # Generate Summary
    # ========================================================================
    print("\n" + "=" * 70)
    print("PROCESSING SUMMARY")
    print("=" * 70)
    
    total_chunks = sum(r.get('chunks', 0) for r in results.values())
    
    print("\nResults by source:")
    for source, result in results.items():
        status_icon = {
            'success': '✅',
            'failed': '❌',
            'skipped': '⚠️ '
        }.get(result['status'], '?')
        
        print(f"  {status_icon} {source.upper()}: {result['chunks']:,} chunks ({result['status']})")
        if result['status'] == 'failed':
            print(f"     Error: {result.get('error', 'Unknown')}")
    
    summary = {
        "total_chunks": total_chunks,
        "by_source": {
            source: result['chunks'] 
            for source, result in results.items()
        },
        "status": {
            source: result['status']
            for source, result in results.items()
        },
        "output_files": [
            f"{source}_chunks.jsonl" 
            for source, result in results.items() 
            if result['status'] == 'success'
        ]
    }
    
    # Add detailed TNM information if available
    if 'tnm' in results and 'details' in results['tnm']:
        summary['tnm_details'] = results['tnm']['details']
    
    summary_file = output_dir / "processing_summary.json"
    with open(summary_file, 'w', encoding='utf-8') as f:
        json.dump(summary, f, indent=2)
    
    print(f"\n{'=' * 70}")
    print(f"✅ PROCESSING COMPLETE!")
    print(f"{'=' * 70}")
    print(f"\nTotal chunks: {total_chunks:,}")
    print(f"Output directory: {output_dir}")
    print(f"Summary saved: {summary_file.name}")
    print(f"\nNext steps:")
    print(f"  1. Run inspection: python src/inspect_chunks.py")
    print(f"  2. Review quality metrics")
    print(f"  3. Validate chunk contents")
    print(f"{'=' * 70}\n")


if __name__ == "__main__":
    main()

