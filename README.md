# Oncology Imaging RAG Data Pipeline

A comprehensive data pipeline for building RAG (Retrieval-Augmented Generation) systems focused on oncologic imaging and response assessment. This repository fetches, processes, and structures medical data from multiple authoritative sources into vector database-ready formats.

## Quick Start

```bash
# Fetch and process all data sources
python fetch_and_process_all.py --all

# Or run individual steps
python fetch_journals.py                 # Fetch PubMed journal abstracts
python fetch_rad_corpora.py --all        # Fetch terminology and guidelines  
python process_abstracts_for_rag.py      # Process abstracts for RAG
python process_rad_data_for_rag.py       # Process terminology for RAG
python process_pdfs_for_rag.py           # Process guidelines for RAG
```

## Data Sources

This pipeline integrates data from multiple authoritative medical sources:

### 1. **PubMed Journal Abstracts** (~292K abstracts, 1968-2025)
- **29 major journals** in oncology and radiology
- Includes: Radiology, European Radiology, Lancet Oncology, JAMA Oncology, Cancer Research, and more
- **All published abstracts** per journal (comprehensive historical coverage)
- **643K text chunks** optimized for embeddings
- Source: NCBI PubMed via E-utilities API

### 2. **RadLex Terminology** (22,747 terms)
- Comprehensive radiology lexicon from RSNA
- 1,800+ detailed definitions, 8,200+ synonyms
- Covers anatomy, imaging modalities, findings, and procedures
- Source: radlex.org

### 3. **LOINC Radiology Playbook** (6,973 procedures)
- Standardized radiology procedure codes
- 190,215 synonyms and naming variations
- RadLex mappings for cross-referencing
- Source: loinc.org (requires free registration)

### 4. **RECIST 1.1 Guidelines** (79 chunks, 20 pages)
- International standard for tumor response assessment
- Detailed measurement protocols and response categories
- Source: EORTC Clinical Guidelines

### 5. **iRECIST** (for immunotherapy response)
- Guidelines for response assessment during immunotherapy
- Source: PMC/Lancet Oncology

### 6. **TNM Cancer Staging 9th Edition - Lung Cancer Protocol**
- AJCC/UICC TNM Classification of Malignant Tumours (2024)
- Comprehensive lung cancer staging documentation
- Detailed T (tumor), N (node), M (metastasis) definitions for lung cancer
- Clinical and pathological staging criteria
- Treatment recommendations and prognostic factors
- Source: AJCC Lung Cancer Staging Protocol

## Output Format

All data is processed into two RAG-optimized formats:

### RAG Documents (`*_rag_documents.json`)
Full documents with metadata, ready for vector database ingestion.

### Text Chunks (`*_text_chunks.json`)
Pre-chunked text (~800-1000 chars) optimized for embedding models.

See [`RAG_CORPORA_DESCRIPTION.md`](RAG_CORPORA_DESCRIPTION.md) for detailed data descriptions and statistics.

## Documentation

- **[USAGE.md](USAGE.md)** - Complete usage guide with examples
- **[RAG_CORPORA_DESCRIPTION.md](RAG_CORPORA_DESCRIPTION.md)** - Detailed data source descriptions
- **[PIPELINE_SUMMARY.md](PIPELINE_SUMMARY.md)** - End-to-end implementation recap with architecture, metrics, and next steps
- **[MARKDOWN_CHUNKING_GUIDE.md](MARKDOWN_CHUNKING_GUIDE.md)** - Overview of the markdown-specific chunking strategy with TNM results and best practices
- **Individual scripts** - Each script has detailed docstrings

## Scripts Overview

| Script | Purpose | Output |
|--------|---------|--------|
| `fetch_and_process_all.py` | Master orchestration script | Runs all fetch and process steps |
| `fetch_journals.py` | Fetch journal abstracts (all time) | CSV files per journal |
| `fetch_rad_corpora.py` | Fetch terminology & guidelines | Raw OWL, CSV, PDF files |
| `rag_pipeline.py process-abstracts` | Process abstracts | RAG-ready JSON documents |
| `process_rad_data_for_rag.py` | Process RadLex & LOINC | RAG-ready JSON documents |
| `rag_pipeline.py process-pdfs` | Process RECIST guidelines | RAG-ready JSON chunks |
| `rag_pipeline.py process-tnm` | Process TNM staging | RAG-ready JSON chunks |

## Requirements

```bash
pip install requests pymupdf
```

**Optional:** Set NCBI API key for faster fetching (3 req/s vs 2 req/s):
```bash
export NCBI_API_KEY="your_key"
export NCBI_EMAIL="your_email@example.com"
```

Get an API key at: https://www.ncbi.nlm.nih.gov/account/

## Directory Structure

```
oncology-imaging-rag/
├── fetch_and_process_all.py       # Master orchestration script
├── fetch_journals.py               # Fetch journal abstracts
├── fetch_rad_corpora.py            # Fetch terminology & guidelines
├── process_abstracts_for_rag.py    # Process abstracts
├── process_rad_data_for_rag.py     # Process terminology
├── process_pdfs_for_rag.py         # Process PDFs
├── README.md                       # This file
├── USAGE.md                        # Detailed usage guide
├── RAG_CORPORA_DESCRIPTION.md      # Data source descriptions
└── data/
    ├── radlex/
    │   └── processed/              # RAG-ready RadLex data
    ├── loinc/
    │   └── processed/              # RAG-ready LOINC data
    ├── guidelines/
    │   └── processed/              # RAG-ready RECIST/iRECIST
    └── pubmed_abstracts/
        ├── *.csv                   # Individual journal CSVs
        └── processed/              # RAG-ready abstract data
```

## Use Cases

- **Medical Question Answering**: Retrieve relevant medical knowledge for clinical queries
- **Clinical Decision Support**: Reference standardized terminology and guidelines
- **Research Literature Search**: Semantic search across oncology and radiology papers
- **Tumor Response Assessment**: Query RECIST criteria and response evaluation protocols
- **Cancer Staging**: Accurate TNM staging determination and classification
- **Terminology Standardization**: Map varied medical terms to standard RadLex/LOINC codes
- **Historical Analysis**: Track evolution of medical concepts from 1968 to present

## Data Statistics

- **Total Documents**: ~322,000
- **Total Text Chunks**: ~674,000
- **Temporal Coverage**: 1968-2025 (57 years)
- **Journals**: 29 major oncology and radiology journals
- **RadLex Terms**: 22,747 with 8,200+ synonyms
- **LOINC Procedures**: 6,973 with 190,000+ synonyms
- **TNM Staging**: 9th Edition (2024) complete staging criteria

## Data Quality

Each processing script generates statistics files (`*_statistics.json`) with:
- Document counts and distributions
- Text length statistics
- Coverage metrics
- Quality indicators

Review these files to assess data quality for your use case.

## License & Attribution

This repository contains processing scripts only. Please cite original data sources:

- **RadLex**: RSNA, Creative Commons Attribution 4.0
- **LOINC**: Regenstrief Institute, LOINC Terms of Use
- **RECIST**: EORTC, European Journal of Cancer
- **PubMed**: NLM/NCBI, individual articles retain original copyright

See [`RAG_CORPORA_DESCRIPTION.md`](RAG_CORPORA_DESCRIPTION.md) for detailed citations.

## Contributing

This pipeline is designed to be extensible:
- Add new journals to `fetch_journals.py`
- Add new data sources to `fetch_rad_corpora.py`
- Customize chunking strategies in processing scripts

## Support

For questions or issues:
1. Check `USAGE.md` for detailed instructions
2. Review `*_statistics.json` files for data quality metrics
3. Examine script docstrings for implementation details

## Updates

### Latest Processing (November 2025)
- ✅ Fetched 292,298 abstracts from 29 journals (1968-2025)
- ✅ Processed 643,910 text chunks
- ✅ RadLex 22,747 terms ready
- ✅ LOINC 6,973 procedures with synonyms ready
- ✅ RECIST 1.1 guidelines processed
- ✅ TNM 9th Edition Lung Cancer Protocol with comprehensive staging criteria
