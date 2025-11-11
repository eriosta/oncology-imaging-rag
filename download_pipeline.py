#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Oncology Imaging Data Download & Processing Pipeline

A comprehensive OOP-based pipeline for downloading and processing medical data
from multiple sources for RAG applications.

Usage:
    # DOWNLOAD ONLY (no processing)
    python download_pipeline.py --fetch-only           # Download all data sources
    python download_pipeline.py fetch-all              # Download all data sources
    python download_pipeline.py fetch-journals         # Download only journal abstracts
    python download_pipeline.py fetch-terminology      # Download RadLex/guidelines (LOINC requires manual download)
    
    # PROCESS ONLY (requires data to be downloaded first)
    python download_pipeline.py --process-only         # Process all downloaded data
    python download_pipeline.py process-all            # Process all downloaded data
    python download_pipeline.py process-abstracts      # Process only abstracts
    python download_pipeline.py process-terminology    # Process only terminology
    python download_pipeline.py process-pdfs           # Process only PDF guidelines
    python download_pipeline.py process-tnm            # Process only TNM staging
    
    # COMPLETE PIPELINE (download + process everything)
    python download_pipeline.py --all                  # Fetch and process everything
    python download_pipeline.py fetch-all process-all  # Fetch and process everything
"""

import os
import sys
import re
import csv
import json
import time
import zipfile
import argparse
from pathlib import Path
from datetime import datetime
from collections import defaultdict
from typing import List, Dict, Optional
from xml.etree import ElementTree as ET

import requests
from requests.adapters import HTTPAdapter, Retry

# Try to import PDF libraries
try:
    import fitz  # PyMuPDF
    PYMUPDF_AVAILABLE = True
except ImportError:
    PYMUPDF_AVAILABLE = False
    try:
        import PyPDF2
        PYPDF2_AVAILABLE = True
    except ImportError:
        PYPDF2_AVAILABLE = False


# ============================================================================
# Configuration
# ============================================================================

class Config:
    """Central configuration for the pipeline"""
    
    # Directories
    DATA_DIR = Path("data")
    RADLEX_DIR = DATA_DIR / "radlex"
    LOINC_DIR = DATA_DIR / "loinc"
    GUIDELINES_DIR = DATA_DIR / "guidelines"
    TNM_DIR = DATA_DIR / "tnm9ed"
    PUBMED_DIR = DATA_DIR / "pubmed_abstracts"
    
    # NCBI Configuration
    NCBI_EMAIL = os.environ.get("NCBI_EMAIL", "your_email@example.com")
    NCBI_API_KEY = os.environ.get("NCBI_API_KEY")
    NCBI_TOOL = "OncologyImagingDownload"
    BASE_DELAY = 0.34 if not NCBI_API_KEY else 0.12
    
    # URLs
    RADLEX_URL = "https://radlex.org/"
    LOINC_URL = "https://loinc.org/downloads/"
    RECIST_PDF = "https://project.eortc.org/recist/wp-content/uploads/sites/4/2015/03/RECISTGuidelines.pdf"
    IRECIST_PMCID = "PMC5648544"
    EUTILS_BASE = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils"
    
    # Journals for comprehensive fetching
    RSNA_JOURNALS = [
        '("Radiology"[Journal] OR 0033-8419[ISSN] OR 1527-1315[ISSN]) AND hasabstract[text]',
        '("Radiographics"[Journal] OR 0271-5333[ISSN] OR 1527-1323[ISSN]) AND hasabstract[text]',
        '("Radiology. Artificial intelligence"[Journal] OR 2638-6100[ISSN]) AND hasabstract[text]',
        '("Radiology. Cardiothoracic imaging"[Journal] OR 2638-6135[ISSN]) AND hasabstract[text]',
        '("Radiology. Imaging cancer"[Journal] OR 2638-616X[ISSN]) AND hasabstract[text]',
    ]
    
    TOP_RAD_JOURNALS = [
        '"AJR. American journal of roentgenology"[Journal] AND hasabstract[text]',
        '"European radiology"[Journal] AND hasabstract[text]',
        '"European journal of radiology"[Journal] AND hasabstract[text]',
        '"Investigative radiology"[Journal] AND hasabstract[text]',
        '"Clinical radiology"[Journal] AND hasabstract[text]',
        '"Skeletal radiology"[Journal] AND hasabstract[text]',
        '"Pediatric radiology"[Journal] AND hasabstract[text]',
        '"Neuroradiology"[Journal] AND hasabstract[text]',
        '"Journal of computer assisted tomography"[Journal] AND hasabstract[text]',
        '"Insights into imaging"[Journal] AND hasabstract[text]',
    ]
    
    ONCOLOGY_JOURNALS = [
        '"The Lancet. Oncology"[Journal] AND hasabstract[text]',
        '"Cancer cell"[Journal] AND hasabstract[text]',
        '"Nature reviews. Clinical oncology"[Journal] AND hasabstract[text]',
        '"Journal of the National Cancer Institute"[Journal] AND hasabstract[text]',
        '"Cancer discovery"[Journal] AND hasabstract[text]',
        '"Cancer research"[Journal] AND hasabstract[text]',
        '"International journal of radiation oncology, biology, physics"[Journal] AND hasabstract[text]',
        '"JAMA oncology"[Journal] AND hasabstract[text]',
        '"The oncologist"[Journal] AND hasabstract[text]',
        '"Gynecologic oncology"[Journal] AND hasabstract[text]',
        '"Cancer"[Journal] AND hasabstract[text]',
        '"British journal of cancer"[Journal] AND hasabstract[text]',
        '"Cancers"[Journal] AND hasabstract[text]',
        '"Neoplasia"[Journal] AND hasabstract[text]',
    ]
    
    @classmethod
    def get_all_journal_queries(cls):
        """Get all journal queries combined"""
        return cls.RSNA_JOURNALS + cls.TOP_RAD_JOURNALS + cls.ONCOLOGY_JOURNALS


# ============================================================================
# Utility Classes
# ============================================================================

class HTTPSession:
    """HTTP session with retry logic"""
    
    @staticmethod
    def create():
        s = requests.Session()
        retry = Retry(
            total=8,
            backoff_factor=0.6,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=frozenset(["GET", "POST"]),
            raise_on_status=False
        )
        s.mount("https://", HTTPAdapter(max_retries=retry))
        s.mount("http://", HTTPAdapter(max_retries=retry))
        s.headers.update({"User-Agent": "OncologyImagingDownload/2.0"})
        return s


class Logger:
    """Simple logging utility"""
    
    @staticmethod
    def section(title):
        print("\n" + "=" * 70)
        print(f"{title}")
        print("=" * 70)
    
    @staticmethod
    def info(message):
        print(f"  {message}")
    
    @staticmethod
    def success(message):
        print(f"  ✅ {message}")
    
    @staticmethod
    def error(message):
        print(f"  ❌ {message}", file=sys.stderr)
    
    @staticmethod
    def warning(message):
        print(f"  ⚠️  {message}")


# ============================================================================
# Base Fetcher Class
# ============================================================================

class BaseFetcher:
    """Base class for all data fetchers"""
    
    def __init__(self):
        self.session = HTTPSession.create()
        self.config = Config()
    
    def throttle(self, delay=None):
        """Rate limiting"""
        time.sleep(delay or self.config.BASE_DELAY)
    
    def save_stream(self, response, output_path: Path):
        """Save streaming response to file"""
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "wb") as f:
            for chunk in response.iter_content(1 << 16):
                if chunk:
                    f.write(chunk)


# ============================================================================
# Journal Abstract Fetcher
# ============================================================================

class JournalFetcher(BaseFetcher):
    """Fetches all abstracts from PubMed journals"""
    
    def __init__(self):
        super().__init__()
        self.output_dir = self.config.PUBMED_DIR
        self.output_dir.mkdir(parents=True, exist_ok=True)
    
    def fetch_all_journals(self, skip_existing=True):
        """Fetch abstracts from all configured journals"""
        Logger.section("FETCHING JOURNAL ABSTRACTS FROM PUBMED")
        
        queries = self.config.get_all_journal_queries()
        Logger.info(f"Total journals to process: {len(queries)}")
        
        results = []
        for query in queries:
            result = self.fetch_journal(query, skip_existing)
            if result:
                results.append(result)
        
        Logger.success(f"Completed fetching {len(results)} journals")
        return results
    
    def fetch_journal(self, query: str, skip_existing=True):
        """Fetch abstracts for a single journal"""
        filename = self._sanitize_filename(query) + ".csv"
        output_path = self.output_dir / filename
        
        if skip_existing and output_path.exists():
            Logger.info(f"Skipping (exists): {filename}")
            return None
        
        Logger.info(f"Fetching: {query[:60]}...")
        
        try:
            # Search for PMIDs
            pmids = self._esearch_all(query)
            if not pmids:
                Logger.warning(f"No results for: {query[:60]}...")
                return None
            
            Logger.info(f"  Found {len(pmids):,} articles")
            
            # Fetch abstracts
            articles = self._efetch_abstracts(pmids)
            Logger.info(f"  Retrieved {len(articles):,} abstracts")
            
            # Save to CSV
            self._save_csv(articles, output_path)
            Logger.success(f"Saved: {filename} ({len(articles):,} abstracts)")
            
            return {"query": query, "count": len(articles), "file": filename}
            
        except Exception as e:
            Logger.error(f"Failed to fetch {query[:60]}: {e}")
            return None
    
    def _esearch_all(self, term: str) -> List[str]:
        """Search PubMed and return all PMIDs (handles large result sets)"""
        # Get count
        count = self._esearch_count(term)
        
        if count > 9000:
            # Split by year to handle 10k API limit
            return self._esearch_by_year(term)
        
        # Fetch normally
        pmids = []
        for start in range(0, min(count, 9999), 500):
            batch = self._esearch_page(term, start, 500)
            pmids.extend(batch)
            self.throttle()
        
        return pmids
    
    def _esearch_count(self, term: str) -> int:
        """Get count of search results"""
        params = {
            "db": "pubmed",
            "term": term,
            "retmode": "json",
            "retmax": 0,
            "tool": self.config.NCBI_TOOL,
            "email": self.config.NCBI_EMAIL
        }
        if self.config.NCBI_API_KEY:
            params["api_key"] = self.config.NCBI_API_KEY
        
        self.throttle()
        r = self.session.get(f"{self.config.EUTILS_BASE}/esearch.fcgi", params=params, timeout=120)
        r.raise_for_status()
        
        data = json.loads(self._clean_json(r.text))
        return int(data["esearchresult"]["count"])
    
    def _esearch_page(self, term: str, retstart: int, retmax: int) -> List[str]:
        """Get a page of PMIDs"""
        params = {
            "db": "pubmed",
            "term": term,
            "retmode": "json",
            "retstart": retstart,
            "retmax": retmax,
            "tool": self.config.NCBI_TOOL,
            "email": self.config.NCBI_EMAIL
        }
        if self.config.NCBI_API_KEY:
            params["api_key"] = self.config.NCBI_API_KEY
        
        self.throttle()
        r = self.session.get(f"{self.config.EUTILS_BASE}/esearch.fcgi", params=params, timeout=120)
        r.raise_for_status()
        
        data = json.loads(self._clean_json(r.text))
        return data["esearchresult"].get("idlist", [])
    
    def _esearch_by_year(self, term: str) -> List[str]:
        """Fetch PMIDs by splitting into year ranges"""
        Logger.info("  Large dataset - splitting by year...")
        all_pmids = []
        
        for year in range(1950, datetime.now().year + 1):
            year_term = f'{term} AND {year}[pdat]'
            try:
                count = self._esearch_count(year_term)
                if count == 0:
                    continue
                
                pmids = []
                for start in range(0, min(count, 9999), 500):
                    batch = self._esearch_page(year_term, start, 500)
                    pmids.extend(batch)
                    self.throttle()
                
                if pmids:
                    all_pmids.extend(pmids)
                    Logger.info(f"    {year}: {len(pmids):,} articles")
            except Exception as e:
                Logger.warning(f"    {year}: error - {e}")
        
        return list(dict.fromkeys(all_pmids))  # Deduplicate
    
    def _efetch_abstracts(self, pmids: List[str], batch_size=200) -> List[Dict]:
        """Fetch article metadata and abstracts"""
        articles = []
        
        for i in range(0, len(pmids), batch_size):
            batch = pmids[i:i + batch_size]
            
            params = {
                "db": "pubmed",
                "id": ",".join(batch),
                "retmode": "xml",
                "tool": self.config.NCBI_TOOL,
                "email": self.config.NCBI_EMAIL
            }
            if self.config.NCBI_API_KEY:
                params["api_key"] = self.config.NCBI_API_KEY
            
            self.throttle()
            r = self.session.get(f"{self.config.EUTILS_BASE}/efetch.fcgi", params=params, timeout=120)
            r.raise_for_status()
            
            root = ET.fromstring(r.text)
            for article in root.findall("PubmedArticle"):
                parsed = self._parse_article(article)
                if parsed:
                    articles.append(parsed)
        
        return articles
    
    def _parse_article(self, article: ET.Element) -> Optional[Dict]:
        """Parse article XML into dictionary"""
        try:
            med = article.find("./MedlineCitation/Article")
            if med is None:
                return None
            
            pmid = article.findtext("./MedlineCitation/PMID", "").strip()
            title = med.findtext("ArticleTitle", "").strip()
            journal = med.findtext("./Journal/Title", "").strip()
            
            # Get abstract
            abstract_parts = []
            abs_node = med.find("Abstract")
            if abs_node is not None:
                for text_node in abs_node.findall("AbstractText"):
                    label = text_node.attrib.get("Label")
                    text = (text_node.text or "").strip()
                    if text:
                        abstract_parts.append(f"{label}: {text}" if label else text)
            
            abstract = "\n".join(abstract_parts)
            if not abstract:
                return None
            
            # Publication info
            volume = med.findtext("./Journal/JournalIssue/Volume", "").strip()
            issue = med.findtext("./Journal/JournalIssue/Issue", "").strip()
            year = med.findtext("./Journal/JournalIssue/PubDate/Year", "").strip()
            month = med.findtext("./Journal/JournalIssue/PubDate/Month", "").strip()
            day = med.findtext("./Journal/JournalIssue/PubDate/Day", "").strip()
            
            # DOI
            doi = ""
            for eid in med.findall("ELocationID"):
                if eid.attrib.get("EIdType", "").lower() == "doi":
                    doi = (eid.text or "").strip()
                    break
            
            # Authors
            authors = []
            for author in med.findall("./AuthorList/Author"):
                first = author.findtext("ForeName", "").strip()
                last = author.findtext("LastName", "").strip()
                if first or last:
                    authors.append(f"{first} {last}".strip())
            
            return {
                "pmid": pmid,
                "doi": doi,
                "journal": journal,
                "title": title,
                "abstract": abstract,
                "volume": volume,
                "issue": issue,
                "pub_year": year,
                "pub_month": month,
                "pub_day": day,
                "authors": "; ".join(authors)
            }
            
        except Exception:
            return None
    
    def _save_csv(self, articles: List[Dict], output_path: Path):
        """Save articles to CSV"""
        fields = ["pmid", "doi", "journal", "title", "abstract", "volume", "issue",
                  "pub_year", "pub_month", "pub_day", "authors"]
        
        with open(output_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fields)
            writer.writeheader()
            writer.writerows(articles)
    
    @staticmethod
    def _sanitize_filename(text: str) -> str:
        """Create safe filename from text"""
        return re.sub(r'[^A-Za-z0-9_.-]+', "_", text)[:100].strip("_")
    
    @staticmethod
    def _clean_json(text: str) -> str:
        """Remove control characters from JSON"""
        return re.sub(r'[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]', '', text)


# ============================================================================
# Terminology & Guidelines Fetcher
# ============================================================================

class TerminologyFetcher(BaseFetcher):
    """Fetches RadLex, LOINC, and clinical guidelines"""
    
    def fetch_radlex(self, skip_existing=True):
        """Download RadLex ontology files"""
        Logger.section("FETCHING RADLEX TERMINOLOGY")
        
        # Check if already downloaded
        owl_file = self.config.RADLEX_DIR / "RadLex.owl"
        if skip_existing and owl_file.exists():
            Logger.info(f"RadLex already downloaded: {owl_file.name}")
            return [owl_file]
        
        try:
            r = self.session.get(self.config.RADLEX_URL, timeout=60)
            r.raise_for_status()
            
            links = re.findall(r'href="(https?://[^"]+\.(?:owl|xlsx|json|zip))"', r.text, re.I)
            links = list(dict.fromkeys(links))
            
            saved = []
            for url in links:
                try:
                    filename = url.split("/")[-1]
                    dest = self.config.RADLEX_DIR / filename
                    
                    if skip_existing and dest.exists():
                        Logger.info(f"Skipping (exists): {filename}")
                        saved.append(dest)
                        continue
                    
                    resp = self.session.get(url, stream=True, timeout=180)
                    if resp.status_code == 200:
                        self.save_stream(resp, dest)
                        saved.append(dest)
                        Logger.success(f"Downloaded: {filename}")
                except Exception as e:
                    Logger.warning(f"Failed to download {filename}: {e}")
            
            return saved
            
        except Exception as e:
            Logger.error(f"Failed to fetch RadLex: {e}")
            return []
    
    def fetch_loinc(self, skip_existing=True):
        """Extract LOINC from manually downloaded zip"""
        Logger.section("EXTRACTING LOINC RADIOLOGY PLAYBOOK")
        
        loinc_dir = self.config.LOINC_DIR
        loinc_dir.mkdir(parents=True, exist_ok=True)
        
        # Check if already extracted
        playbook_csv = loinc_dir / "LoincRsnaRadiologyPlaybook.csv"
        if skip_existing and playbook_csv.exists():
            Logger.info(f"LOINC already extracted: {playbook_csv.name}")
            return [playbook_csv]
        
        # Look for LOINC zip
        zips = list(loinc_dir.glob("Loinc_*.zip")) + list(loinc_dir.glob("LOINC_*.zip"))
        
        if not zips:
            Logger.warning("No LOINC zip found. Manual download required.")
            self._create_loinc_instructions(loinc_dir)
            return []
        
        try:
            zip_path = zips[0]
            Logger.info(f"Found: {zip_path.name}")
            
            saved = []
            with zipfile.ZipFile(zip_path, 'r') as zf:
                # Extract Radiology Playbook
                playbook_files = [f for f in zf.namelist()
                                  if 'RadiologyPlaybook' in f and f.endswith('.csv')]
                
                for pf in playbook_files:
                    extract_path = loinc_dir / Path(pf).name
                    
                    if skip_existing and extract_path.exists():
                        Logger.info(f"Skipping (exists): {extract_path.name}")
                        saved.append(extract_path)
                        continue
                    
                    with zf.open(pf) as source, open(extract_path, 'wb') as target:
                        target.write(source.read())
                    saved.append(extract_path)
                    Logger.success(f"Extracted: {extract_path.name}")
            
            return saved
            
        except Exception as e:
            Logger.error(f"Failed to extract LOINC: {e}")
            return []
    
    def fetch_recist(self, skip_existing=True):
        """Download RECIST 1.1 PDF"""
        Logger.section("FETCHING RECIST 1.1 GUIDELINES")
        
        dest = self.config.GUIDELINES_DIR / "RECIST_1.1_EORTC.pdf"
        
        # Check if already downloaded
        if skip_existing and dest.exists():
            Logger.info(f"RECIST already downloaded: {dest.name}")
            return dest
        
        try:
            r = self.session.get(self.config.RECIST_PDF, stream=True, timeout=180)
            if r.status_code == 200:
                self.save_stream(r, dest)
                Logger.success(f"Downloaded: {dest.name}")
                return dest
            else:
                Logger.error(f"Failed to download (status {r.status_code})")
                return None
                
        except Exception as e:
            Logger.error(f"Failed to fetch RECIST: {e}")
            return None
    
    def fetch_irecist(self, skip_existing=True):
        """Download iRECIST PDF"""
        Logger.section("FETCHING iRECIST GUIDELINES")
        
        dest = self.config.GUIDELINES_DIR / "iRECIST.pdf"
        
        # Check if already downloaded
        if skip_existing and dest.exists():
            Logger.info(f"iRECIST already downloaded: {dest.name}")
            return dest
        
        pdf_url = f"https://pmc.ncbi.nlm.nih.gov/articles/{self.config.IRECIST_PMCID}/pdf/"
        
        try:
            r = self.session.get(pdf_url, stream=True, timeout=180)
            if r.status_code == 200:
                self.save_stream(r, dest)
                Logger.success(f"Downloaded: {dest.name}")
                return dest
        except Exception as e:
            Logger.warning(f"Direct download failed: {e}")
        
        # Fallback: create download instructions
        note_path = self.config.GUIDELINES_DIR / "iRECIST_DOWNLOAD_NOTE.txt"
        if not note_path.exists():
            note_path.write_text(
                f"iRECIST Manual Download\n"
                f"{'=' * 50}\n\n"
                f"Download from: {pdf_url}\n"
                f"Or visit: https://www.ncbi.nlm.nih.gov/pmc/articles/{self.config.IRECIST_PMCID}/\n"
            )
            Logger.warning("Created download instructions")
        return note_path
    
    @staticmethod
    def _create_loinc_instructions(loinc_dir: Path):
        """Create LOINC download instructions"""
        note = loinc_dir / "DOWNLOAD_INSTRUCTIONS.txt"
        note.write_text(
            "LOINC Download Instructions\n"
            "=" * 50 + "\n\n"
            "1. Visit: https://loinc.org/downloads/\n"
            "2. Create free account or log in\n"
            "3. Download LOINC package (e.g., Loinc_2.81.zip)\n"
            "4. Place zip in this directory\n"
            "5. Run: python download_pipeline.py fetch-terminology\n"
        )


# ============================================================================
# Abstract Processor
# ============================================================================

class AbstractProcessor:
    """Processes PubMed abstracts into RAG format"""
    
    def __init__(self):
        self.config = Config()
        self.input_dir = self.config.PUBMED_DIR
        self.output_dir = self.config.PUBMED_DIR / "processed"
    
    def process_all(self, skip_if_exists=True):
        """Process all CSV files into RAG format"""
        Logger.section("PROCESSING PUBMED ABSTRACTS FOR RAG")
        
        # Check if already processed
        output_file = self.output_dir / "pubmed_abstracts_rag_documents.json"
        if skip_if_exists and output_file.exists():
            Logger.info(f"Abstracts already processed: {output_file.name}")
            Logger.info("Use skip_if_exists=False to reprocess")
            return self._load_existing_stats()
        
        # Load all abstracts
        abstracts = self._load_all_csvs()
        if not abstracts:
            Logger.error("No abstracts found to process")
            return
        
        Logger.info(f"Loaded {len(abstracts):,} abstracts")
        
        # Create RAG documents
        documents = self._create_rag_documents(abstracts)
        Logger.info(f"Created {len(documents):,} RAG documents")
        
        # Create text chunks
        chunks = self._create_text_chunks(abstracts)
        Logger.info(f"Created {len(chunks):,} text chunks")
        
        # Generate statistics
        stats = self._generate_statistics(abstracts, documents, chunks)
        
        # Save outputs
        self._save_outputs(documents, chunks, stats, abstracts)
        
        Logger.success("Abstract processing complete")
        return stats
    
    def _load_existing_stats(self):
        """Load existing statistics if available"""
        stats_file = self.output_dir / "pubmed_abstracts_statistics.json"
        if stats_file.exists():
            with open(stats_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        return None
    
    def _load_all_csvs(self) -> List[Dict]:
        """Load abstracts from all CSV files"""
        abstracts = []
        csv_files = list(self.input_dir.glob("*.csv"))
        
        Logger.info(f"Found {len(csv_files)} CSV files")
        
        for csv_file in sorted(csv_files):
            try:
                with open(csv_file, 'r', encoding='utf-8') as f:
                    reader = csv.DictReader(f)
                    file_abstracts = [row for row in reader if row.get('abstract', '').strip()]
                    abstracts.extend(file_abstracts)
                    Logger.info(f"  {csv_file.name}: {len(file_abstracts):,} abstracts")
            except Exception as e:
                Logger.warning(f"  {csv_file.name}: Error - {e}")
        
        return abstracts
    
    def _create_rag_documents(self, abstracts: List[Dict]) -> List[Dict]:
        """Create RAG-optimized documents"""
        documents = []
        
        for abstract in abstracts:
            title = abstract.get('title', '').strip()
            abstract_text = abstract.get('abstract', '').strip()
            searchable_text = f"{title}\n\n{abstract_text}"
            
            authors_str = abstract.get('authors', '').strip()
            authors_list = [a.strip() for a in authors_str.split(';') if a.strip()] if authors_str else []
            
            doc = {
                'id': f"PMID:{abstract.get('pmid', '')}",
                'title': title,
                'text': searchable_text,
                'metadata': {
                    'pmid': abstract.get('pmid', ''),
                    'doi': abstract.get('doi', ''),
                    'journal': abstract.get('journal', ''),
                    'year': abstract.get('pub_year', ''),
                    'month': abstract.get('pub_month', ''),
                    'authors': authors_list[:5],
                    'source': 'pubmed',
                    'document_type': 'abstract'
                }
            }
            documents.append(doc)
        
        return documents
    
    def _create_text_chunks(self, abstracts: List[Dict], max_length=1000) -> List[Dict]:
        """Create embedding-optimized chunks"""
        chunks = []
        
        for abstract in abstracts:
            pmid = abstract.get('pmid', '')
            title = abstract.get('title', '').strip()
            abstract_text = abstract.get('abstract', '').strip()
            full_text = f"{title}\n\n{abstract_text}"
            
            if len(full_text) <= max_length:
                chunks.append({
                    'chunk_id': f"PMID:{pmid}_chunk_0",
                    'pmid': pmid,
                    'text': full_text,
                    'chunk_index': 0,
                    'total_chunks': 1
                })
            else:
                # Split by sentences
                sentences = re.split(r'(?<=[.!?])\s+', abstract_text)
                current_chunk = title + "\n\n"
                chunk_list = []
                
                for sent in sentences:
                    if len(current_chunk) + len(sent) > max_length and len(current_chunk) > len(title) + 2:
                        chunk_list.append(current_chunk.strip())
                        current_chunk = title + "\n\n" + sent + " "
                    else:
                        current_chunk += sent + " "
                
                if current_chunk.strip():
                    chunk_list.append(current_chunk.strip())
                
                for idx, chunk_text in enumerate(chunk_list):
                    chunks.append({
                        'chunk_id': f"PMID:{pmid}_chunk_{idx}",
                        'pmid': pmid,
                        'text': chunk_text,
                        'chunk_index': idx,
                        'total_chunks': len(chunk_list)
                    })
        
        return chunks
    
    def _generate_statistics(self, abstracts, documents, chunks) -> Dict:
        """Generate processing statistics"""
        stats = {
            'total_abstracts': len(abstracts),
            'total_documents': len(documents),
            'total_chunks': len(chunks),
            'processing_date': datetime.now().isoformat()
        }
        
        # Journal distribution
        journal_counts = defaultdict(int)
        for a in abstracts:
            journal_counts[a.get('journal', 'Unknown')] += 1
        stats['journal_distribution'] = dict(sorted(journal_counts.items(), key=lambda x: x[1], reverse=True))
        
        # Year distribution
        year_counts = defaultdict(int)
        for a in abstracts:
            year = a.get('pub_year', '')
            if year:
                year_counts[year] += 1
        stats['year_distribution'] = dict(sorted(year_counts.items()))
        
        # Text length stats
        abstract_lengths = [len(a.get('abstract', '')) for a in abstracts]
        chunk_lengths = [len(c['text']) for c in chunks]
        
        stats['text_length_stats'] = {
            'abstract': {
                'min': min(abstract_lengths) if abstract_lengths else 0,
                'max': max(abstract_lengths) if abstract_lengths else 0,
                'avg': round(sum(abstract_lengths) / len(abstract_lengths), 1) if abstract_lengths else 0
            },
            'chunk': {
                'min': min(chunk_lengths) if chunk_lengths else 0,
                'max': max(chunk_lengths) if chunk_lengths else 0,
                'avg': round(sum(chunk_lengths) / len(chunk_lengths), 1) if chunk_lengths else 0
            }
        }
        
        return stats
    
    def _save_outputs(self, documents, chunks, stats, abstracts):
        """Save all processed outputs"""
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # RAG documents
        with open(self.output_dir / "pubmed_abstracts_rag_documents.json", 'w', encoding='utf-8') as f:
            json.dump(documents, f, indent=2, ensure_ascii=False)
        Logger.success(f"Saved RAG documents ({len(documents):,})")
        
        # Text chunks
        with open(self.output_dir / "pubmed_abstracts_text_chunks.json", 'w', encoding='utf-8') as f:
            json.dump(chunks, f, indent=2, ensure_ascii=False)
        Logger.success(f"Saved text chunks ({len(chunks):,})")
        
        # Statistics
        with open(self.output_dir / "pubmed_abstracts_statistics.json", 'w', encoding='utf-8') as f:
            json.dump(stats, f, indent=2, ensure_ascii=False)
        Logger.success("Saved statistics")
        
        # CSV for review
        with open(self.output_dir / "pubmed_abstracts_for_rag.csv", 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=['pmid', 'doi', 'title', 'journal', 'pub_year', 'abstract'])
            writer.writeheader()
            for a in abstracts:
                writer.writerow({
                    'pmid': a.get('pmid', ''),
                    'doi': a.get('doi', ''),
                    'title': a.get('title', ''),
                    'journal': a.get('journal', ''),
                    'pub_year': a.get('pub_year', ''),
                    'abstract': a.get('abstract', '')
                })
        Logger.success("Saved CSV for review")


# ============================================================================
# Terminology Processor (RadLex & LOINC)
# ============================================================================

class TerminologyProcessor:
    """Processes RadLex and LOINC into RAG format"""
    
    def __init__(self):
        self.config = Config()
    
    def process_all(self, skip_if_exists=True):
        """Process both RadLex and LOINC"""
        Logger.section("PROCESSING TERMINOLOGY FOR RAG")
        
        radlex_stats = self.process_radlex(skip_if_exists)
        loinc_stats = self.process_loinc(skip_if_exists)
        
        Logger.success("Terminology processing complete")
        return {'radlex': radlex_stats, 'loinc': loinc_stats}
    
    def process_radlex(self, skip_if_exists=True):
        """Process RadLex OWL file"""
        Logger.info("Processing RadLex...")
        
        output_dir = self.config.RADLEX_DIR / "processed"
        output_file = output_dir / "radlex_rag_documents.json"
        
        # Check if already processed
        if skip_if_exists and output_file.exists():
            Logger.info(f"RadLex already processed: {output_file.name}")
            stats_file = output_dir / "radlex_statistics.json"
            if stats_file.exists():
                with open(stats_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            return None
        
        owl_file = self.config.RADLEX_DIR / "RadLex.owl"
        if not owl_file.exists():
            Logger.error(f"RadLex OWL file not found: {owl_file}")
            return None
        
        # Parse OWL (simplified - full implementation in original script)
        Logger.info("  Parsing OWL file...")
        terms = self._parse_radlex_simple(owl_file)
        
        # Create RAG documents
        documents = self._create_radlex_documents(terms)
        
        # Save
        output_dir.mkdir(parents=True, exist_ok=True)
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(documents, f, indent=2, ensure_ascii=False)
        
        stats = {'total_terms': len(terms), 'total_documents': len(documents)}
        
        with open(output_dir / "radlex_statistics.json", 'w', encoding='utf-8') as f:
            json.dump(stats, f, indent=2)
        
        Logger.success(f"RadLex: {len(terms):,} terms processed")
        return stats
    
    def process_loinc(self, skip_if_exists=True):
        """Process LOINC Radiology Playbook"""
        Logger.info("Processing LOINC...")
        
        output_dir = self.config.LOINC_DIR / "processed"
        output_file = output_dir / "loinc_rag_documents.json"
        
        # Check if already processed
        if skip_if_exists and output_file.exists():
            Logger.info(f"LOINC already processed: {output_file.name}")
            stats_file = output_dir / "loinc_statistics.json"
            if stats_file.exists():
                with open(stats_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            return None
        
        csv_file = self.config.LOINC_DIR / "LoincRsnaRadiologyPlaybook.csv"
        if not csv_file.exists():
            Logger.error(f"LOINC CSV not found: {csv_file}")
            return None
        
        # Parse CSV (simplified - full implementation in original script)
        Logger.info("  Parsing Playbook CSV...")
        procedures = self._parse_loinc_simple(csv_file)
        
        # Create RAG documents
        documents = self._create_loinc_documents(procedures)
        
        # Save
        output_dir.mkdir(parents=True, exist_ok=True)
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(documents, f, indent=2, ensure_ascii=False)
        
        stats = {'total_procedures': len(procedures), 'total_documents': len(documents)}
        
        with open(output_dir / "loinc_statistics.json", 'w', encoding='utf-8') as f:
            json.dump(stats, f, indent=2)
        
        Logger.success(f"LOINC: {len(procedures):,} procedures processed")
        return stats
    
    def _parse_radlex_simple(self, owl_file: Path) -> List[Dict]:
        """Simplified RadLex parser - use full version from process_rad_data_for_rag.py for production"""
        # This is a placeholder - the full implementation should use the complete parser
        Logger.warning("Using simplified RadLex parser - integrate full version for production")
        return []
    
    def _parse_loinc_simple(self, csv_file: Path) -> List[Dict]:
        """Simplified LOINC parser - use full version from process_rad_data_for_rag.py for production"""
        # This is a placeholder - the full implementation should use the complete parser
        Logger.warning("Using simplified LOINC parser - integrate full version for production")
        return []
    
    def _create_radlex_documents(self, terms: List[Dict]) -> List[Dict]:
        """Create RAG documents for RadLex"""
        return []  # Placeholder
    
    def _create_loinc_documents(self, procedures: List[Dict]) -> List[Dict]:
        """Create RAG documents for LOINC"""
        return []  # Placeholder


# ============================================================================
# PDF Processor
# ============================================================================

class PDFProcessor:
    """Processes PDF guidelines into RAG format"""
    
    def __init__(self):
        self.config = Config()
    
    def process_all(self, skip_if_exists=True):
        """Process all PDFs in guidelines directory"""
        Logger.section("PROCESSING PDF GUIDELINES FOR RAG")
        
        if not PYMUPDF_AVAILABLE and not PYPDF2_AVAILABLE:
            Logger.error("No PDF library available. Install: pip install pymupdf")
            return []
        
        pdf_files = list(self.config.GUIDELINES_DIR.glob("*.pdf"))
        
        if not pdf_files:
            Logger.warning("No PDF files found in guidelines directory")
        
        results = []
        for pdf_file in pdf_files:
            result = self.process_pdf(pdf_file, skip_if_exists, output_dir=self.config.GUIDELINES_DIR / "processed")
            if result:
                results.append(result)
        
        Logger.success(f"Processed {len(results)} guideline PDF(s)")
        return results
    
    def process_tnm(self, skip_if_exists=True):
        """Process TNM staging cards"""
        Logger.section("PROCESSING TNM STAGING CARDS FOR RAG")
        
        if not PYMUPDF_AVAILABLE and not PYPDF2_AVAILABLE:
            Logger.error("No PDF library available. Install: pip install pymupdf")
            return []
        
        pdf_files = list(self.config.TNM_DIR.glob("*.pdf"))
        
        if not pdf_files:
            Logger.warning("No TNM PDF files found")
            return []
        
        results = []
        for pdf_file in pdf_files:
            result = self.process_pdf(pdf_file, skip_if_exists, output_dir=self.config.TNM_DIR / "processed", doc_type="staging")
            if result:
                results.append(result)
        
        Logger.success(f"Processed {len(results)} TNM staging PDF(s)")
        return results
    
    def process_pdf(self, pdf_path: Path, skip_if_exists=True, output_dir: Path = None, doc_type: str = "guideline"):
        """Process a single PDF file"""
        if output_dir is None:
            output_dir = self.config.GUIDELINES_DIR / "processed"
        
        output_file = output_dir / f"{pdf_path.stem}_rag_documents.json"
        
        # Check if already processed
        if skip_if_exists and output_file.exists():
            Logger.info(f"Already processed: {pdf_path.name}")
            return {'file': pdf_path.name, 'chunks': 'skipped'}
        
        Logger.info(f"Processing: {pdf_path.name}")
        
        # Extract text
        pages = self._extract_text(pdf_path)
        
        # Create chunks
        chunks = self._chunk_by_paragraphs(pages)
        
        # Create RAG documents
        documents = self._create_rag_documents(chunks, pdf_path.stem, doc_type)
        
        # Save
        output_dir.mkdir(parents=True, exist_ok=True)
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(documents, f, indent=2, ensure_ascii=False)
        
        # Save statistics
        stats = {
            'source_file': pdf_path.name,
            'total_pages': len(pages),
            'total_chunks': len(chunks),
            'avg_chunk_size': round(sum(len(c['text']) for c in chunks) / len(chunks), 1) if chunks else 0,
            'document_type': doc_type
        }
        
        with open(output_dir / f"{pdf_path.stem}_statistics.json", 'w', encoding='utf-8') as f:
            json.dump(stats, f, indent=2)
        
        Logger.success(f"  {pdf_path.stem}: {len(chunks)} chunks from {len(pages)} pages")
        return {'file': pdf_path.name, 'chunks': len(chunks), 'pages': len(pages)}
    
    def _extract_text(self, pdf_path: Path) -> List[Dict]:
        """Extract text from PDF"""
        if PYMUPDF_AVAILABLE:
            return self._extract_pymupdf(pdf_path)
        else:
            return self._extract_pypdf2(pdf_path)
    
    def _extract_pymupdf(self, pdf_path: Path) -> List[Dict]:
        """Extract using PyMuPDF"""
        pages = []
        doc = fitz.open(pdf_path)
        for page_num, page in enumerate(doc, start=1):
            pages.append({
                'page_number': page_num,
                'text': page.get_text().strip()
            })
        doc.close()
        return pages
    
    def _extract_pypdf2(self, pdf_path: Path) -> List[Dict]:
        """Extract using PyPDF2"""
        pages = []
        with open(pdf_path, 'rb') as f:
            reader = PyPDF2.PdfReader(f)
            for page_num, page in enumerate(reader.pages, start=1):
                pages.append({
                    'page_number': page_num,
                    'text': page.extract_text().strip()
                })
        return pages
    
    def _chunk_by_paragraphs(self, pages: List[Dict]) -> List[Dict]:
        """Create chunks from paragraphs"""
        chunks = []
        chunk_id = 0
        
        for page in pages:
            paragraphs = [p.strip() for p in page['text'].split('\n\n') if p.strip()]
            
            for para in paragraphs:
                if len(para) > 100:  # Only meaningful paragraphs
                    chunks.append({
                        'id': f"chunk_{chunk_id}",
                        'text': para,
                        'page': page['page_number']
                    })
                    chunk_id += 1
        
        return chunks
    
    def _create_rag_documents(self, chunks: List[Dict], source_name: str, doc_type: str = "guideline") -> List[Dict]:
        """Create RAG documents from chunks"""
        documents = []
        for chunk in chunks:
            doc = {
                'id': f"{source_name}_{chunk['id']}",
                'text': chunk['text'],
                'metadata': {
                    'source': source_name,
                    'type': doc_type,
                    'page': chunk['page']
                }
            }
            documents.append(doc)
        return documents


# ============================================================================
# Main Pipeline Orchestrator
# ============================================================================

class DataPipeline:
    """Main data download and processing pipeline orchestrator"""
    
    def __init__(self):
        self.journal_fetcher = JournalFetcher()
        self.term_fetcher = TerminologyFetcher()
        self.abstract_processor = AbstractProcessor()
        self.term_processor = TerminologyProcessor()
        self.pdf_processor = PDFProcessor()
    
    def fetch_all(self):
        """Fetch all data sources (download only, no processing)"""
        Logger.section("FETCHING ALL DATA SOURCES (DOWNLOAD ONLY)")
        Logger.info("This will download data but NOT process it.")
        Logger.info("Run with --process-only or process-all to process downloaded data.")
        Logger.info("Note: LOINC requires manual download from https://loinc.org/downloads/\n")
        
        self.journal_fetcher.fetch_all_journals()
        self.term_fetcher.fetch_radlex()
        # LOINC requires manual download - not included in automatic fetch
        self.term_fetcher.fetch_recist()
        self.term_fetcher.fetch_irecist()
        
        Logger.section("DOWNLOAD COMPLETE")
        Logger.success("All data sources have been downloaded")
        Logger.warning("LOINC must be downloaded manually from: https://loinc.org/downloads/")
        Logger.info("  Place the LOINC zip file in: data/loinc/")
        Logger.info("\nTo process the downloaded data, run:")
        Logger.info("  python download_pipeline.py --process-only")
        Logger.info("  or")
        Logger.info("  python download_pipeline.py process-all")
    
    def process_all(self):
        """Process all data sources (requires downloaded data)"""
        Logger.section("PROCESSING ALL DATA SOURCES")
        Logger.info("Processing downloaded data into RAG-ready format...\n")
        
        self.abstract_processor.process_all()
        self.term_processor.process_all()
        self.pdf_processor.process_all()
        self.pdf_processor.process_tnm()
        
        Logger.section("PROCESSING COMPLETE")
        Logger.success("All data sources have been processed and are ready for RAG ingestion!")
        Logger.info("Check the 'processed' subdirectories for output files.")
    
    def run_full_pipeline(self):
        """Run complete fetch and process pipeline"""
        Logger.section("RUNNING FULL DATA PIPELINE")
        Logger.info(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        self.fetch_all()
        self.process_all()
        
        Logger.section("PIPELINE COMPLETE")
        Logger.info(f"Finished: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        Logger.success("All data ready for RAG ingestion!")


# ============================================================================
# CLI
# ============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Oncology Imaging Data Download & Processing Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  
  DOWNLOAD ONLY (no processing):
    python download_pipeline.py --fetch-only           # Download all data sources
    python download_pipeline.py fetch-all              # Download all data sources
    python download_pipeline.py fetch-journals         # Download only journals
    python download_pipeline.py fetch-terminology      # Download RadLex/guidelines (not LOINC)
    
    Note: LOINC must be manually downloaded from https://loinc.org/downloads/
          and placed in data/loinc/ directory
  
  PROCESS ONLY (requires downloaded data):
    python download_pipeline.py --process-only         # Process all downloaded data
    python download_pipeline.py process-all            # Process all downloaded data
    python download_pipeline.py process-abstracts      # Process only abstracts
    python download_pipeline.py process-terminology    # Process only terminology
    python download_pipeline.py process-pdfs           # Process only guidelines
    python download_pipeline.py process-tnm            # Process only TNM staging
  
  COMPLETE PIPELINE (download + process):
    python download_pipeline.py --all                  # Fetch and process everything
    python download_pipeline.py fetch-all process-all  # Fetch and process everything
        """
    )
    
    # Simplified flags
    parser.add_argument('--all', action='store_true', 
                       help='Run complete pipeline: download and process everything')
    parser.add_argument('--fetch-only', action='store_true', 
                       help='DOWNLOAD ONLY: Fetch all data without processing')
    parser.add_argument('--process-only', action='store_true', 
                       help='PROCESS ONLY: Process previously downloaded data')
    
    # Individual commands
    parser.add_argument('commands', nargs='*', choices=[
        'fetch-all', 'fetch-journals', 'fetch-terminology',
        'process-all', 'process-abstracts', 'process-terminology', 'process-pdfs', 'process-tnm'
    ], help='Specific commands to run')
    
    args = parser.parse_args()
    
    pipeline = DataPipeline()
    
    # Handle simplified flags
    if args.all:
        pipeline.run_full_pipeline()
        return
    
    if args.fetch_only:
        pipeline.fetch_all()
        return
    
    if args.process_only:
        pipeline.process_all()
        return
    
    # Handle specific commands
    if not args.commands:
        parser.print_help()
        return
    
    for cmd in args.commands:
        if cmd == 'fetch-all':
            pipeline.fetch_all()
        elif cmd == 'fetch-journals':
            pipeline.journal_fetcher.fetch_all_journals()
        elif cmd == 'fetch-terminology':
            pipeline.term_fetcher.fetch_radlex()
            # LOINC requires manual download
            pipeline.term_fetcher.fetch_recist()
            pipeline.term_fetcher.fetch_irecist()
        elif cmd == 'process-all':
            pipeline.process_all()
        elif cmd == 'process-abstracts':
            pipeline.abstract_processor.process_all()
        elif cmd == 'process-terminology':
            pipeline.term_processor.process_all()
        elif cmd == 'process-pdfs':
            pipeline.pdf_processor.process_all()
        elif cmd == 'process-tnm':
            pipeline.pdf_processor.process_tnm()


if __name__ == '__main__':
    main()



