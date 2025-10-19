# RAG Corpora for Oncologic Imaging & Response Assessment

## Overview

This repository contains a comprehensive, RAG-ready corpus focused on **oncologic imaging** and **response assessment** in cancer treatment. The data spans medical terminology, clinical guidelines, and current research literature, providing a robust foundation for retrieval-augmented generation systems.

**Last Updated:** October 2025  
**Total Documents:** ~31,500  
**Total Text Chunks:** ~5,000  
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

## 4. PubMed Research Literature (2023-2025)

**Source:** PubMed abstracts via NCBI E-utilities  
**Location:** `data/pubmed_abstracts/processed/`

### Description
Current research literature from top-tier oncology and radiology journals, covering recent advances in cancer imaging, response assessment, and treatment monitoring.

### Content
- **1,767 abstracts** from 2023-2025
- **14 premier journals** including:
  1. **European Radiology** - 941 abstracts
  2. **Radiology** - 299 abstracts
  3. **Clinical Cancer Research** - 152 abstracts
  4. **European Journal of Cancer** - 78 abstracts
  5. **Journal of Clinical Oncology** - 78 abstracts
  6. **American Journal of Roentgenology** - 46 abstracts
  7. **The Lancet Oncology** - 42 abstracts
  8. **JAMA Oncology** - 38 abstracts
  9. Plus Nature, Cancer Cell, Cancer Research, and more

### Query Categories

#### a. **Oncologic Imaging** (1,424 abstracts)
General cancer imaging research across all modalities

#### b. **RECIST Response Assessment** (304 abstracts)
Studies specifically about RECIST criteria, tumor measurement, and response evaluation

#### c. **Immunotherapy Response** (4 abstracts)
Imaging response to immunotherapy (iRECIST, irRC, immune-related patterns)

#### d. **PET Response Criteria** (6 abstracts)
PET-specific response criteria (PERCIST, Deauville, etc.)

#### e. **Radiology Report Analysis** (11 abstracts)
Natural language processing and structured reporting

#### f. **Temporal Progression** (18 abstracts)
Longitudinal imaging and disease progression tracking

### Temporal Coverage
- **2023:** 643 abstracts
- **2024:** 589 abstracts
- **2025:** 535 abstracts (through October)

### RAG Format
- **Structure:**
  - Title + Abstract combined for full searchable text
  - PMID for unique identification
  - Journal, year, authors in metadata
  - 4,899 optimized text chunks (avg 800 chars)

- **Files per category:**
  - `abstracts_rag_documents.json` - Full documents with metadata
  - `abstracts_text_chunks.json` - Embeddings-optimized chunks
  - `abstracts_statistics.json` - Processing metadata
  - `abstracts_for_rag.csv` - Human-readable format

### Use Cases
- Finding recent research on specific imaging topics
- Understanding current best practices
- Discovering novel imaging biomarkers
- Tracking research trends and methodologies

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

### `fetch_rad_corpora.py`
Fetches and downloads source data:
- PubMed abstracts via NCBI E-utilities
- Handles LOINC zip extraction
- Manages RadLex OWL files
- Downloads clinical guideline PDFs

### `process_rad_data_for_rag.py`
Processes terminology data:
- Parses RadLex OWL ontology
- Extracts LOINC procedure codes
- Enriches with synonyms and mappings
- Generates RAG-ready JSON documents

### `process_pdfs_for_rag.py`
Processes PDF documents:
- Extracts text from clinical guidelines
- Detects sections and paragraphs
- Creates optimized text chunks
- Preserves document structure

### `process_abstracts_for_rag.py`
Processes literature abstracts:
- Combines title + abstract text
- Chunks longer abstracts intelligently
- Generates statistics and distributions
- Creates both document and chunk formats

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
| **RadLex** | 22,747 terms | 1,800 definitions, 8,237 synonyms | N/A |
| **LOINC** | 6,973 procedures | 190,215 synonyms, full RadLex mapping | N/A |
| **RECIST 1.1** | 79 chunks | 20 pages, 43 sections | 79 |
| **PubMed** | 1,767 abstracts | 14 journals, 2023-2025 | 4,899 |
| **TOTAL** | **~31,566** | | **~4,978** |

---

## Maintenance

### Updating Literature
```bash
# Fetch latest abstracts (runs all recommended queries)
python fetch_rad_corpora.py --pubmed

# Process new abstracts
python process_abstracts_for_rag.py
```

### Adding Custom Queries
Edit `RECOMMENDED_QUERIES` in `fetch_rad_corpora.py` or use:
```bash
python fetch_rad_corpora.py --freeform "your custom query here"
```

### Updating Terminology
- RadLex: Download new OWL file from radlex.org
- LOINC: Download new zip from loinc.org
- Rerun processing scripts

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

## Contact & Support

For questions about this corpus or to report issues:
- Review processing scripts for implementation details
- Check `*_statistics.json` files for data quality metrics
- Regenerate processed data if source files are updated

**Domain Focus:** Oncologic imaging, tumor response assessment, radiology terminology, clinical trial standards

