# RAG Corpora for Oncologic Imaging & Response Assessment

## Overview

This repository contains a comprehensive, RAG-ready corpus focused on **oncologic imaging** and **response assessment** in cancer treatment. The data spans medical terminology, clinical guidelines, and current research literature, providing a robust foundation for retrieval-augmented generation systems.

**Last Updated:** October 2025  
**Total Documents:** ~321,000  
**Total Text Chunks:** ~650,000  
**Temporal Coverage:** 1968-2025 (57 years)  
**Focus Areas:** Radiology, oncology, imaging response criteria, tumor assessment

---

## 1. RadLex Ontology (Terminology)

**Source:** RadLex 4.2 (Radiological Society of North America)  
**Location:** `data/radlex/processed/`

### Description
RadLex is the comprehensive radiology lexicon developed by RSNA. It provides standardized terminology for radiology practice, education, and research.

### Content
- **22,747 radiology terms** including:
  - Anatomical entities
  - Imaging modalities (CT, MRI, PET, ultrasound)
  - Imaging observations and findings
  - Procedures and techniques
  - Pathological processes
  
- **1,800 terms with detailed definitions**
- **5,809 terms with synonyms** (8,237 total synonyms)

### RAG Format
- **Primary file:** `radlex_rag_documents.json`
- Each document contains:
  - Term ID and preferred label
  - Definitions and synonyms
  - Hierarchical relationships (parent/child concepts)
  - Metadata (source, type)

### Use Cases
- Medical term disambiguation
- Standardizing radiology report language
- Understanding anatomical and imaging terminology
- Query expansion with synonyms

---

## 2. LOINC Radiology Playbook (Terminology)

**Source:** LOINC (Logical Observation Identifiers Names and Codes)  
**Location:** `data/loinc/processed/`

### Description
LOINC-RSNA Radiology Playbook provides standardized codes for radiology procedures and reports, enabling consistent identification and exchange of radiology information.

### Content
- **6,973 radiology procedures** fully coded with:
  - LOINC codes and long names
  - RadLex anatomical mappings
  - Imaging modality codes
  - Document types and guidance

- **190,215 synonyms** (avg 27.3 per procedure)
  - Multiple naming variations per procedure
  - Historical and alternative names
  - Common abbreviations

### Enrichment
Each procedure includes:
- Component breakdowns (what is being imaged)
- Property and timing information
- System/anatomical location
- Method/modality details
- Status codes

### RAG Format
- **Primary file:** `loinc_rag_documents.json`
- Each document contains:
  - LOINC code and full procedure name
  - All synonyms and variations
  - RadLex mappings
  - Searchable text combining all elements

### Use Cases
- Procedure name normalization
- Mapping clinical orders to standard codes
- Understanding procedure variations
- Converting between naming conventions

---

## 3. RECIST 1.1 Clinical Guidelines

**Source:** RECIST 1.1 / EORTC Clinical Guidelines PDF  
**Location:** `data/guidelines/processed/`

### Description
Response Evaluation Criteria In Solid Tumours (RECIST) version 1.1 is the international standard for assessing tumor response to treatment in oncology clinical trials and practice.

### Content
- **20 pages** of comprehensive clinical guidance
- **79 text chunks** covering:
  - Tumor measurement methodology
  - Target and non-target lesion definitions
  - Response categories (CR, PR, SD, PD)
  - Lymph node measurement guidelines
  - Documentation requirements
  - Special cases and considerations

### Structure
- Original PDF extracted and parsed with PyMuPDF
- Text chunked into semantic paragraphs
- Section headers preserved for context
- Average chunk size: 1,243 characters

### RAG Format
- **Primary files:**
  - `RECIST_1.1_EORTC_rag_documents.json` - Full document chunks
  - `RECIST_1.1_EORTC_text_chunks.json` - Optimized for embeddings

### Use Cases
- Understanding tumor response assessment
- Clarifying RECIST measurement rules
- Training on clinical trial standards
- Answering specific guideline questions

---

## 4. PubMed Research Literature (1968-2025)

**Source:** PubMed abstracts via NCBI E-utilities  
**Location:** `data/pubmed_abstracts/processed/`

### Description
**Comprehensive historical research literature** from top-tier oncology and radiology journals, providing both foundational knowledge and cutting-edge research spanning nearly 6 decades.

### Content
- **292,298 abstracts** from 1968-2025 (all time coverage)
- **29 premier journals** including:
  1. **Cancer Research** - 45,851 abstracts
  2. **Cancer** - 36,724 abstracts
  3. **Cancers** - 31,208 abstracts
  4. **Int. J. Radiation Oncology** - 23,076 abstracts
  5. **British Journal of Cancer** - 22,538 abstracts
  6. **Gynecologic Oncology** - 15,538 abstracts
  7. **European Radiology** - 9,999 abstracts
  8. **Radiology** - 9,928 abstracts
  9. **AJR** - 8,647 abstracts
  10. Plus Lancet Oncology, JAMA Oncology, Nature Reviews, Cancer Cell, and more

### Journal Categories

#### a. **RSNA Journals** (5 journals, ~21K abstracts)
- Radiology, Radiographics, Radiology: AI, Radiology: Cardiothoracic Imaging, Radiology: Imaging Cancer

#### b. **Top Radiology Journals** (10 journals, ~75K abstracts)
- European Radiology, AJR, Clinical Radiology, Investigative Radiology, Skeletal Radiology, and more

#### c. **Oncology Journals** (14 journals, ~196K abstracts)
- Cancer Research, Cancer, British Journal of Cancer, Lancet Oncology, JAMA Oncology, and more

### Temporal Coverage (Historical to Current)
- **1968-1974:** 515 abstracts (early foundations)
- **1975-1989:** 38,467 abstracts (growth period)
- **1990-1999:** 43,010 abstracts (expansion)
- **2000-2009:** 66,057 abstracts (digital era)
- **2010-2019:** 79,417 abstracts (modern advances)
- **2020-2025:** 64,832 abstracts (current research)
  - **2023:** 12,048 abstracts
  - **2024:** 10,353 abstracts  
  - **2025:** 8,928 abstracts (through October)

### RAG Format
- **Structure:**
  - Title + Abstract combined for full searchable text
  - PMID for unique identification
  - DOI, journal, year, authors, volume, issue in metadata
  - 643,910 optimized text chunks (avg 764 chars)

- **Files (unified across all journals):**
  - `pubmed_abstracts_rag_documents.json` - 292,298 full documents with metadata
  - `pubmed_abstracts_text_chunks.json` - 643,910 embeddings-optimized chunks
  - `pubmed_abstracts_statistics.json` - Processing metadata with distributions
  - `pubmed_abstracts_for_rag.csv` - Human-readable format for review

### Use Cases
- **Historical Context:** Access foundational papers that established current standards
- **Recent Research:** Find cutting-edge advances from 2023-2025
- **Longitudinal Analysis:** Track evolution of imaging techniques across decades
- **Comprehensive Search:** Query across 57 years of medical literature
- **Best Practices:** Understand both historical development and current standards
- **Novel Biomarkers:** Discover emerging imaging biomarkers
- **Trend Analysis:** Track research trends and methodologies over time

---

## Data Format Specifications

### RAG Document Structure

All processed data follows a consistent structure optimized for vector databases:

```json
{
  "id": "unique_identifier",
  "title": "Document title or term",
  "text": "Full searchable text content",
  "metadata": {
    "source": "pubmed|radlex|loinc|recist",
    "document_type": "abstract|term|guideline",
    "year": "2024",
    "... other relevant metadata ..."
  }
}
```

### Text Chunk Structure

For embedding optimization:

```json
{
  "chunk_id": "doc_id_chunk_N",
  "text": "Chunk text content (optimal length for embeddings)",
  "chunk_index": 0,
  "total_chunks": 3,
  "... source metadata ..."
}
```

---

## Processing Scripts

### `rag_pipeline.py` (Main Pipeline)
**Consolidated OOP-based pipeline** that handles all data fetching and processing:

**Fetching:**
- PubMed journal abstracts via NCBI E-utilities (all time coverage)
- RadLex OWL ontology files
- LOINC Radiology Playbook extraction
- RECIST and iRECIST guideline PDFs

**Processing:**
- PubMed abstracts → RAG documents and text chunks
- PDF guidelines → chunked RAG documents
- Smart skip logic (automatically skips already downloaded/processed data)

**Features:**
- Clean OOP architecture with modular classes
- Automatic detection of existing data (no redundant work)
- Simple CLI with multiple operation modes
- Comprehensive error handling and logging

### `process_rad_data_for_rag.py` (Terminology Processing)
**Specialized processor for RadLex and LOINC**:
- Parses RadLex OWL ontology (full XML parsing)
- Extracts LOINC procedure codes with enrichment
- Processes synonyms and hierarchical relationships
- Generates comprehensive RAG-ready JSON documents
- Can be run standalone or integrated with main pipeline

---

## Usage Recommendations

### For Vector Database Ingestion

1. **Use `*_rag_documents.json` files** for primary document storage
2. **Index the `text` field** for semantic search
3. **Store `metadata` separately** for filtering and attribution
4. **Use `id` field** as primary key

### For Embedding Generation

1. **Use `*_text_chunks.json` files** for chunked content
2. **Chunks are pre-sized** for optimal embedding performance (~800 chars)
3. **Maintain `chunk_id`** to reassemble documents
4. **Include metadata** for context-aware retrieval

### For Hybrid Search

1. **Terminology (RadLex, LOINC):** Exact + synonym matching
2. **Guidelines (RECIST):** Semantic + keyword search
3. **Literature (PubMed):** Recency-weighted semantic search

### Query Strategy Suggestions

- **Medical terms:** Check RadLex/LOINC first for standardization
- **Clinical procedures:** Query RECIST guidelines + relevant literature
- **Current research:** Filter PubMed by year and journal
- **Comprehensive answers:** Combine all sources with ranked fusion

---

## Statistics Summary

| Corpus | Documents | Additional Metrics | Text Chunks |
|--------|-----------|-------------------|-------------|
| **RadLex** | 22,747 terms | 1,800 definitions, 8,237 synonyms | ~22,747 |
| **LOINC** | 6,973 procedures | 190,215 synonyms, full RadLex mapping | ~6,973 |
| **RECIST 1.1** | 79 chunks | 20 pages, 43 sections | 79 |
| **PubMed** | 292,298 abstracts | 29 journals, 1968-2025, 57 years | 643,910 |
| **TOTAL** | **~322,097** | | **~673,709** |

---

## Maintenance

### Quick Start
```bash
# Fetch and process everything (first time)
python rag_pipeline.py --all

# Update with new data (automatically skips existing)
python rag_pipeline.py --all
```

### Updating Literature
```bash
# Fetch latest journal abstracts (skips existing journals)
python rag_pipeline.py fetch-journals

# Process new abstracts (skips if already processed)
python rag_pipeline.py process-abstracts
```

### Selective Operations
```bash
# Fetch only
python rag_pipeline.py --fetch-only

# Process only (data already fetched)
python rag_pipeline.py --process-only

# Specific components
python rag_pipeline.py fetch-terminology
python rag_pipeline.py process-pdfs
```

### Updating Terminology
```bash
# Download new RadLex/LOINC files
python rag_pipeline.py fetch-terminology

# Process with full parser
python process_rad_data_for_rag.py
```

### Force Reprocessing
To force reprocessing of existing data:
1. Delete the specific output file (e.g., `pubmed_abstracts_rag_documents.json`)
2. Run the pipeline again - it will regenerate

### Adding Custom Journals
Edit the `RSNA_JOURNALS`, `TOP_RAD_JOURNALS`, or `ONCOLOGY_JOURNALS` lists in the `Config` class of `rag_pipeline.py`

---

## Citation & Attribution

### RadLex
- Radiological Society of North America (RSNA)
- RadLex Version 4.2
- License: Creative Commons Attribution 4.0
- URL: https://radlex.org

### LOINC
- Regenstrief Institute, Inc.
- LOINC-RSNA Radiology Playbook
- License: LOINC Terms of Use
- URL: https://loinc.org

### RECIST 1.1
- European Organisation for Research and Treatment of Cancer (EORTC)
- Published: European Journal of Cancer
- Citation: Eisenhauer EA, et al. Eur J Cancer. 2009;45(2):228-247

### PubMed
- National Library of Medicine (NLM)
- Accessed via NCBI E-utilities
- Public domain abstracts
- Individual articles retain original copyright

---

## Pipeline Features

### Smart Skip Logic
The pipeline automatically detects and skips:
- ✅ Already downloaded journal CSVs
- ✅ Already fetched RadLex/LOINC files
- ✅ Already downloaded PDFs
- ✅ Already processed abstracts
- ✅ Already processed guidelines

**Result:** Second run completes in seconds (vs hours first time)

### Incremental Updates
```bash
# Fetch new journals (skips existing)
python rag_pipeline.py fetch-journals

# Process only new abstracts
python rag_pipeline.py process-abstracts
```

### Resume Capability
Interrupted pipelines can resume from where they stopped - no need to restart from scratch.

## Contact & Support

For questions about this corpus or to report issues:
- Review `rag_pipeline.py` for implementation details
- Check `*_statistics.json` files for data quality metrics
- Use `--help` flag for usage information
- Regenerate processed data by deleting output files and re-running

**Domain Focus:** Oncologic imaging, tumor response assessment, radiology terminology, clinical trial standards

**Repository:** Consolidated OOP-based pipeline with smart skip logic and comprehensive error handling

