# RAG Data Processing Pipeline - Source Code

This directory contains the modular processing pipeline for transforming medical terminology and clinical guidelines into RAG-optimized chunks.

## Quick Start

```bash
# Process all data sources
python3 src/main.py

# Inspect generated chunks
python3 src/inspect_chunks.py
```

## Architecture

```
src/
├── models/              # Data models
│   ├── chunk.py         # Pydantic Chunk model
│   └── __init__.py
├── processing/          # Data processors
│   ├── radlex_processor.py    # RadLex ontology
│   ├── loinc_processor.py     # LOINC terminology  
│   ├── pdf_processor.py       # TNM & RECIST PDFs
│   └── __init__.py
├── main.py             # Pipeline orchestration
├── inspect_chunks.py   # Quality inspection tool
└── README.md           # This file
```

## Processors

### RadLexProcessor
**Input:** `data/radlex/extracted/RadLex.owl`  
**Output:** `output/processed_chunks/radlex_chunks.jsonl`  
**Strategy:** One term per chunk with full context

```python
from processing.radlex_processor import RadLexProcessor
from pathlib import Path

processor = RadLexProcessor(Path("data/radlex/extracted"))
chunks = processor.process()
processor.save_chunks(Path("output/radlex_chunks.jsonl"))
```

**Chunk Format:**
```
RadLex ID: RID12345
Term: Hepatic Steatosis
Definition: Accumulation of fat in liver cells...
Synonyms: Fatty Liver, Hepatic Lipidosis
Parent terms: RID98765
```

### LOINCProcessor
**Input:** `data/loinc/extracted/.../LoincRsnaRadiologyPlaybook.csv`  
**Output:** `output/processed_chunks/loinc_chunks.jsonl`  
**Strategy:** Group by LOINC code, aggregate anatomical parts

```python
from processing.loinc_processor import LOINCProcessor
from pathlib import Path

processor = LOINCProcessor(Path("data/loinc"))
chunks = processor.process()
processor.save_chunks(Path("output/loinc_chunks.jsonl"))
```

**Chunk Format:**
```
LOINC: 1742-6
Name: Alanine aminotransferase measurement
Component: ALT, Serum
RadLex IDs: RID12345, RID67890
System: Laboratory, Blood Chemistry
```

### TNMProcessor
**Input:** `data/tnm9ed/Staging Cards 9th Edition 2024.pdf`  
**Output:** `output/processed_chunks/tnm_chunks.jsonl`  
**Strategy:** Semantic chunking by cancer site and staging component

```python
from processing.pdf_processor import TNMProcessor
from pathlib import Path

processor = TNMProcessor(Path("data/tnm9ed/Staging Cards 9th Edition 2024.pdf"))
chunks = processor.process()
processor.save_chunks(Path("output/tnm_chunks.jsonl"))
```

**Chunk Format:**
```
Lung Cancer T Classification–9th Edition

T1: Tumor ≤ 3cm, surrounded by lung or visceral pleura
T2: Tumor > 3cm, ≤ 5cm OR tumor with specific features...
[Full staging details]
```

### RECISTProcessor
**Input:** `data/guidelines/RECIST_1.1_EORTC.pdf`  
**Output:** `output/processed_chunks/recist_chunks.jsonl`  
**Strategy:** Semantic chunking by section, preserves measurement criteria

```python
from processing.pdf_processor import RECISTProcessor
from pathlib import Path

processor = RECISTProcessor(Path("data/guidelines/RECIST_1.1_EORTC.pdf"))
chunks = processor.process()
processor.save_chunks(Path("output/recist_chunks.jsonl"))
```

**Chunk Format:**
```
3.1 Target Lesions

Target lesions should be selected based on:
- Size (longest diameter for non-nodal lesions)
- Suitability for accurate repeated measurements
- Representative of all involved organs
[Full criteria]
```

## Data Model

### Chunk (Pydantic)

```python
class Chunk(BaseModel):
    chunk_id: str              # Unique identifier
    text: str                  # Content to embed
    source_type: str           # radlex|loinc|tnm_table|recist
    metadata: Dict[str, Any]   # Flexible metadata
    
    def to_dict(self) -> dict
    @classmethod
    def from_dict(cls, data: dict)
```

**Metadata Fields:**
- `source_file`: Original file name
- `created_at`: Processing timestamp (ISO format)
- `char_count`: Text length
- Source-specific fields (see below)

**RadLex Metadata:**
- `term_id`: RadLex ID (e.g., "RID12345")
- `label`: Preferred term
- `has_definition`: Boolean
- `synonym_count`: Number of synonyms
- `parent_count`: Number of parent terms
- `category`: "terminology"

**LOINC Metadata:**
- `code`: LOINC code (e.g., "1742-6")
- `procedure_name`: Long common name
- `anatomical_parts`: List of anatomical regions
- `radlex_ids`: List of RadLex cross-references
- `part_count`: Number of component parts
- `category`: "lab_terminology"

**TNM Metadata:**
- `page`: PDF page number
- `cancer_type`: Identified cancer type
- `cancer_site`: Section header
- `category`: T-staging, N-staging, M-staging, etc.
- `staging_component`: Subsection
- `tnm_edition`: "9th"
- `chunk_method`: "semantic"

**RECIST Metadata:**
- `page`: PDF page number
- `section`: Main section header
- `subsection`: Subsection header
- `chunk_method`: "semantic"

## Output Format

### JSONL (JSON Lines)
One chunk per line, easy to stream and process in batches.

```json
{"chunk_id": "radlex_RID12345", "text": "RadLex ID: RID12345...", "source_type": "radlex", "metadata": {...}}
{"chunk_id": "loinc_1742_6", "text": "LOINC: 1742-6...", "source_type": "loinc", "metadata": {...}}
```

### Processing Summary
`output/processed_chunks/processing_summary.json`

```json
{
  "total_chunks": 53936,
  "by_source": {
    "radlex": 46838,
    "loinc": 6973,
    "tnm": 72,
    "recist": 53
  },
  "status": {
    "radlex": "success",
    "loinc": "success",
    "tnm": "success",
    "recist": "success"
  }
}
```

## Inspection Tool

### Usage
```bash
python3 src/inspect_chunks.py
```

### Output
- File-by-file analysis
- Text length statistics (min, max, mean, median, std dev)
- Distribution across size buckets
- Metadata field inventory
- Sample chunks with previews
- Overall summary

### Example Output
```
======================================================================
INSPECTING: radlex_chunks.jsonl
======================================================================
Total chunks: 46,838

Source types:
  radlex: 46,838

Text length statistics:
  Min: 44 chars
  Max: 1,202 chars
  Mean: 81.9 chars
  Median: 57.0 chars

Length distribution:
  Very short (<100)   : 37,809 ( 80.7%) ████████████████████████████████████████
  Short (100-500)     :  8,869 ( 18.9%) █████████
  Medium (500-1500)   :    160 (  0.3%)
```

## Adding New Data Sources

1. **Create Processor**
   ```python
   # src/processing/my_processor.py
   from models.chunk import Chunk
   from pathlib import Path
   from typing import List
   import json
   
   class MyProcessor:
       def __init__(self, data_path: Path):
           self.data_path = data_path
           self.chunks: List[Chunk] = []
       
       def load_data(self):
           # Load your data
           pass
       
       def process(self) -> List[Chunk]:
           # Transform to chunks
           data = self.load_data()
           for item in data:
               chunk = Chunk(
                   chunk_id=f"my_{item['id']}",
                   text=item['text'],
                   source_type="my_source",
                   metadata={...}
               )
               self.chunks.append(chunk)
           return self.chunks
       
       def save_chunks(self, output_path: Path):
           with open(output_path, 'w') as f:
               for chunk in self.chunks:
                   f.write(json.dumps(chunk.to_dict()) + '\n')
   ```

2. **Register in __init__.py**
   ```python
   # src/processing/__init__.py
   from .my_processor import MyProcessor
   
   __all__ = [..., 'MyProcessor']
   ```

3. **Add to Pipeline**
   ```python
   # src/main.py
   from processing.my_processor import MyProcessor
   
   # In main():
   my_processor = MyProcessor(data_dir / "my_data")
   my_chunks = my_processor.process()
   my_processor.save_chunks(output_dir / "my_chunks.jsonl")
   ```

## Chunking Strategies

### Fixed-Size Chunking
Split text at fixed character/token boundaries.

**Pros:** Simple, predictable size  
**Cons:** May split mid-sentence, loses context

### Paragraph Chunking
Split at paragraph boundaries.

**Pros:** Maintains paragraph coherence  
**Cons:** Variable sizes, may be too small/large

### Semantic Chunking (Used Here)
Split at semantic boundaries (sections, headers).

**Pros:** 
- Preserves document structure
- Maintains context and coherence
- Respects logical boundaries
- Better for retrieval quality

**Cons:**
- More complex implementation
- Variable chunk sizes (mitigated with max_size splits)

**Implementation:**
1. Detect document structure (headers, sections)
2. Group content under each header
3. Include header in chunk for context
4. Split large sections at paragraph boundaries
5. Always include header in sub-chunks

## Performance

### Timing
- RadLex: ~5 seconds (46K terms from OWL)
- LOINC: ~2 seconds (7K procedures from CSV)
- TNM: ~3 seconds (44 pages, 72 chunks)
- RECIST: ~2 seconds (20 pages, 53 chunks)

**Total:** ~12 seconds for all sources

### Memory
- Peak: ~150 MB
- Streaming-friendly (JSONL output)
- Can process sources independently

## Error Handling

All processors include error handling:
- File not found checks
- Parse error recovery
- Validation of required fields
- Graceful degradation (skip vs fail)

Main pipeline catches exceptions:
```python
try:
    processor = RadLexProcessor(...)
    chunks = processor.process()
    results['radlex'] = {'status': 'success', 'chunks': len(chunks)}
except Exception as e:
    results['radlex'] = {'status': 'failed', 'error': str(e)}
```

## Testing

### Manual Validation
```bash
# Process
python3 src/main.py

# Inspect
python3 src/inspect_chunks.py

# Spot check samples
head -5 output/processed_chunks/radlex_chunks.jsonl | python3 -m json.tool
```

### Automated Checks
```python
# Load chunks
with open('output/processed_chunks/radlex_chunks.jsonl') as f:
    chunks = [json.loads(line) for line in f]

# Validate
assert all('chunk_id' in c for c in chunks)
assert all('text' in c and len(c['text']) > 0 for c in chunks)
assert all('source_type' in c for c in chunks)
assert all('metadata' in c for c in chunks)
```

## Dependencies

```
pymupdf>=1.23.0    # PDF extraction
pydantic>=2.0.0    # Data validation
tqdm>=4.65.0       # Progress bars
```

## FAQ

**Q: Why JSONL instead of JSON?**  
A: JSONL allows streaming/batch processing without loading entire file into memory. Each line is a complete JSON object.

**Q: Can I process just one source?**  
A: Yes! Import the processor directly and run it standalone.

**Q: How do I change chunk sizes?**  
A: Modify `max_chunk_size` in PDF processors (default: 3000 chars for semantic sections).

**Q: Where are embeddings generated?**  
A: This pipeline generates text chunks. Embeddings are generated separately using models like BGE-large-en-v1.5.

**Q: Can I use different chunking strategies?**  
A: Yes! Modify the processor's chunking method or create a new processor class.

## License

See main repository LICENSE file.

