# RAG Data Corpora for Oncologic Imaging

A comprehensive, RAG-ready corpus for oncologic imaging and response assessment, combining medical terminology, clinical guidelines, and current research literature.

## 🎯 Quick Start

### Fetch Data
```bash
# Fetch all recommended PubMed queries
python fetch_rad_corpora.py --pubmed

# Or run specific queries
python fetch_rad_corpora.py --freeform "your custom query here"
```

### Process Data for RAG
```bash
# Process terminology (RadLex & LOINC)
python process_rad_data_for_rag.py

# Process PDF guidelines (RECIST)
python process_pdfs_for_rag.py

# Process PubMed abstracts
python process_abstracts_for_rag.py
```

## 📊 Corpus Overview

| Source | Documents | Description |
|--------|-----------|-------------|
| **RadLex** | 22,747 terms | Comprehensive radiology terminology with definitions & synonyms |
| **LOINC** | 6,973 procedures | Standardized radiology procedure codes with 190K synonyms |
| **RECIST 1.1** | 79 chunks | Clinical guidelines for tumor response assessment |
| **PubMed** | 1,767 abstracts | Recent research (2023-2025) from top journals |
| **Total** | **~31,566** | Full RAG-ready corpus |

## 📁 Project Structure

```
rag_data/
├── fetch_rad_corpora.py           # Fetch source data
├── process_rad_data_for_rag.py    # Process terminology
├── process_pdfs_for_rag.py        # Process PDF guidelines
├── process_abstracts_for_rag.py   # Process literature
├── RAG_CORPORA_DESCRIPTION.md     # Detailed documentation
└── data/
    ├── radlex/processed/          # RadLex ontology (RAG-ready)
    ├── loinc/processed/           # LOINC procedures (RAG-ready)
    ├── guidelines/processed/      # RECIST guidelines (RAG-ready)
    └── pubmed_abstracts/processed/ # Literature abstracts (RAG-ready)
```

## 🔧 Requirements

```bash
pip install requests xmltodict pymupdf
```

## 📖 Documentation

See [RAG_CORPORA_DESCRIPTION.md](RAG_CORPORA_DESCRIPTION.md) for:
- Detailed corpus descriptions
- Data format specifications
- Usage recommendations
- Query strategies
- Citation & attribution

## 🎓 Use Cases

- Medical information retrieval
- Clinical decision support
- Radiology report generation
- Research question answering
- Medical term standardization
- Response assessment guidance

## 📝 Citation

This corpus combines data from:
- **RadLex** (RSNA) - radlex.org
- **LOINC** (Regenstrief Institute) - loinc.org
- **RECIST 1.1** (EORTC)
- **PubMed** (NLM/NCBI)

Please cite the original sources when using this data.

## 🚀 Next Steps

1. Load RAG documents into your vector database
2. Generate embeddings from text chunks
3. Implement hybrid search (semantic + keyword)
4. Add metadata filtering for precise retrieval
5. Build your RAG application!

## 📬 Notes

- Source data files (.zip, .pdf, .owl) are not committed to git
- Only processed RAG-ready outputs are versioned
- Run fetch scripts to download source data locally
- Processed files are optimized for vector databases

