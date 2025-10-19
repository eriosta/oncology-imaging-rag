#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Radiology/Oncology corpora fetcher for RAG applications.

Data Sources:
- RadLex: Radiology terminology ontology (~50K terms)
- LOINC Playbook: Radiology procedure codes (49K+ codes)
- RECIST 1.1 / iRECIST: Oncology response assessment guidelines (PDFs)
- PubMed Abstracts: High-quality abstracts from top journals (2023-2025)
  * Top oncology journals: Lancet Oncol, J Clin Oncol, JAMA Oncol, etc.
  * Top radiology journals: Radiology, Eur Radiol, AJR, etc.

Examples:
  # Download all available sources
  python fetch_rad_corpora.py --all
  
  # Individual sources
  python fetch_rad_corpora.py --radlex
  python fetch_rad_corpora.py --loinc
  python fetch_rad_corpora.py --recist --irecist
  
  # Fetch abstracts from top journals (2023-2025)
  python fetch_rad_corpora.py --pmc-queries \\
    --start-date 2023-01-01 \\
    --end-date 2025-12-31 \\
    --all-records

Output Structure:
  data/
    ├── radlex/          # Terminology files
    ├── loinc/           # Procedure codes
    ├── guidelines/      # RECIST PDFs
    └── pubmed_abstracts/  # Abstracts by query category
"""

import os, re, sys, time, json, argparse
from pathlib import Path
from urllib.parse import urlencode
import requests
from requests.adapters import HTTPAdapter, Retry
from xml.etree import ElementTree as ET

# ----------------------------
# Config
# ----------------------------
OUTDIR = Path("data").resolve()
HEADERS = {"User-Agent": "EriDataFetcher/2.0 (research use; contact: you@example.com)"}
NCBI_TOOL = "EriDataFetcher"
NCBI_EMAIL = os.environ.get("NCBI_EMAIL", "you@example.com")  # set valid email
NCBI_API_KEY = os.environ.get("NCBI_API_KEY")  # optional; increases rate
BASE_DELAY = 0.34 if not NCBI_API_KEY else 0.12  # NCBI polite defaults

EUTILS = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils"
RADLEX_HOME = "https://radlex.org/"
RECIST11_PDF = "https://project.eortc.org/recist/wp-content/uploads/sites/4/2015/03/RECISTGuidelines.pdf"
IRECIST_PMCID = "PMC5648544"
LOINC_DOWNLOADS = "https://loinc.org/downloads/"
# Top oncology and radiology journals
TOP_JOURNALS = [
    "Lancet Oncol", "J Clin Oncol", "JAMA Oncol", "Cancer Cell", "Nat Rev Cancer",
    "Cancer Discov", "Ann Oncol", "Radiology", "Eur J Cancer", "Clin Cancer Res",
    "Cancer Res", "J Natl Cancer Inst", "Eur Radiol", "AJR Am J Roentgenol"
]

JOURNAL_FILTER = " OR ".join([f'"{j}"[jour]' for j in TOP_JOURNALS])

RECOMMENDED_QUERIES = [
    ("RECIST response assessment",
     f'(Neoplasms[MeSH]) AND (RECIST[tw] OR "tumor response"[tw]) AND ({JOURNAL_FILTER})'),
    ("Oncologic imaging",
     f'(Neoplasms[MeSH]) AND (Diagnostic Imaging[MeSH]) AND ({JOURNAL_FILTER})'),
    ("Temporal progression",
     f'(Disease Progression[MeSH]) AND (longitudinal[tw] OR serial[tw]) AND (imaging[tw]) AND ({JOURNAL_FILTER})'),
    ("Immunotherapy response",
     f'(Immunotherapy[MeSH]) AND (pseudoprogression[tw] OR iRECIST[tw]) AND ({JOURNAL_FILTER})'),
    ("PET response criteria",
     f'(Positron Emission Tomography[MeSH]) AND (PERCIST[tw] OR "PET response"[tw]) AND ({JOURNAL_FILTER})'),
    ("Radiology report analysis",
     f'(radiology reports[tw] OR radiologic reports[tw]) AND (natural language processing[MeSH]) AND ({JOURNAL_FILTER})'),
    ("Lesion tracking",
     f'(lesion tracking[tw] OR lesion correspondence[tw]) AND (oncology[tw] OR cancer[tw]) AND ({JOURNAL_FILTER})'),
]

# ----------------------------
# HTTP session with retries
# ----------------------------
def session():
    s = requests.Session()
    r = Retry(total=8, backoff_factor=0.6,
              status_forcelist=(429,500,502,503,504),
              allowed_methods=frozenset(["GET","POST"]), raise_on_status=False)
    s.mount("https://", HTTPAdapter(max_retries=r))
    s.mount("http://", HTTPAdapter(max_retries=r))
    s.headers.update(HEADERS)
    return s

S = session()

def throttle(delay):
    time.sleep(delay)

def save_stream(resp, outpath: Path):
    outpath.parent.mkdir(parents=True, exist_ok=True)
    with open(outpath, "wb") as f:
        for chunk in resp.iter_content(1<<16):
            if chunk:
                f.write(chunk)

# ----------------------------
# Date utils
# ----------------------------
def _norm_date(s: str) -> str:
    # E-utilities prefers YYYY/MM/DD. Accept YYYY-MM-DD or YYYY/MM/DD.
    return s.replace("-", "/")

# ----------------------------
# E-utilities helpers (fixed)
# ----------------------------
def eutils_get(path, params, delay=BASE_DELAY):
    q = dict(params)
    q.setdefault("tool", NCBI_TOOL)
    q.setdefault("email", NCBI_EMAIL)
    if NCBI_API_KEY:
        q["api_key"] = NCBI_API_KEY
    throttle(delay)
    r = S.get(f"{EUTILS}/{path}", params=q, timeout=120)
    r.raise_for_status()
    return r

def esearch_count(term, mindate, maxdate):
    r = eutils_get("esearch.fcgi", {
        "db": "pubmed",
        "term": term,
        "retmode": "json",
        "datetype": "pdat",
        "mindate": _norm_date(mindate),
        "maxdate": _norm_date(maxdate),
        "retmax": 0
    })
    js = r.json()
    return int(js["esearchresult"]["count"])

def esearch_page(term, mindate, maxdate, retstart, retmax):
    r = eutils_get("esearch.fcgi", {
        "db": "pubmed",
        "term": term,
        "retmode": "json",
        "datetype": "pdat",
        "mindate": _norm_date(mindate),
        "maxdate": _norm_date(maxdate),
        "retstart": retstart,
        "retmax": retmax
    })
    js = r.json()
    # NCBI errors live under "errorlist"/HTTP status, not "ERROR" field
    if "esearchresult" not in js:
        raise RuntimeError(f"Unexpected ESearch payload: {js.keys()}")
    return js["esearchresult"].get("idlist", [])

def esearch_all(term, mindate, maxdate, page_size=1000, delay=BASE_DELAY):
    # Handles 10k window cap by chunking by year then month if needed.
    total = esearch_count(term, mindate, maxdate)
    if total <= 10000:
        ids, retstart = [], 0
        while retstart < total:
            ids.extend(esearch_page(term, mindate, maxdate, retstart, page_size))
            retstart += page_size
            throttle(delay)
        return ids

    print(f"  Large result set {total:,}. Chunking by year…")
    from datetime import datetime
    y0 = int(_norm_date(mindate).split("/")[0])
    y1 = int(_norm_date(maxdate).split("/")[0])

    all_ids = []
    for y in range(y0, y1 + 1):
        ymind, ymaxd = f"{y}/01/01", f"{y}/12/31"
        ycount = esearch_count(term, ymind, ymaxd)
        if ycount <= 10000:
            # pull year directly
            retstart = 0
            while retstart < ycount:
                all_ids.extend(esearch_page(term, ymind, ymaxd, retstart, page_size))
                retstart += page_size
                throttle(delay)
        else:
            print(f"    Year {y}: {ycount:,}. Chunking by month…")
            for m in range(1, 13):
                mmind = f"{y}/{m:02d}/01"
                mmaxd = f"{y}/{m:02d}/31"
                mcount = esearch_count(term, mmind, mmaxd)
                if mcount == 0:
                    continue
                retstart = 0
                while retstart < mcount:
                    all_ids.extend(esearch_page(term, mmind, mmaxd, retstart, page_size))
                    retstart += page_size
                    throttle(delay)

    # de-dup preserve order
    seen, uniq = set(), []
    for x in all_ids:
        if x not in seen:
            seen.add(x)
            uniq.append(x)
    print(f"  Collected {len(uniq):,} unique PubMed IDs")
    return uniq

# ----------------------------
# Fetch abstracts from PubMed
# ----------------------------
def fetch_abstracts(pmids, batch=200, delay=BASE_DELAY):
    """Fetch article metadata and abstracts for a list of PubMed IDs"""
    abstracts = []
    for i in range(0, len(pmids), batch):
        chunk = pmids[i:i+batch]
        r = eutils_get("efetch.fcgi", {
            "db": "pubmed",
            "id": ",".join(chunk),
            "retmode": "xml"
        }, delay=delay)
        
        root = ET.fromstring(r.text)
        for article in root.findall(".//PubmedArticle"):
            try:
                pmid = article.findtext(".//PMID")
                title = article.findtext(".//ArticleTitle") or ""
                abstract_texts = article.findall(".//AbstractText")
                abstract = " ".join([at.text or "" for at in abstract_texts if at.text])
                
                # Journal info
                journal = article.findtext(".//Journal/Title") or ""
                pub_date_year = article.findtext(".//PubDate/Year") or ""
                
                # Authors
                authors = []
                for author in article.findall(".//Author"):
                    last = author.findtext("LastName") or ""
                    first = author.findtext("ForeName") or ""
                    if last or first:
                        authors.append(f"{first} {last}".strip())
                
                if abstract:  # Only include if abstract exists
                    abstracts.append({
                        "pmid": pmid,
                        "title": title,
                        "abstract": abstract,
                        "journal": journal,
                        "year": pub_date_year,
                        "authors": authors[:5]  # Limit to first 5 authors
                    })
            except Exception:
                continue  # Skip articles that fail to parse
    
    return abstracts

# ----------------------------
# Sources without date filters
# ----------------------------
def download_radlex(outdir: Path):
    r = S.get(RADLEX_HOME, timeout=60)
    r.raise_for_status()
    html = r.text
    links = re.findall(r'href="(https?://[^"]+\.(?:owl|xlsx|json|zip))"', html, re.I)
    links = list(dict.fromkeys(links))
    saved = []
    for url in links:
        try:
            fn = url.split("/")[-1]
            dest = outdir / "radlex" / fn
            rr = S.get(url, stream=True, timeout=180)
            if rr.status_code == 200:
                save_stream(rr, dest)
                saved.append(dest)
        except requests.RequestException:
            pass
    if not saved:
        (outdir / "radlex").mkdir(parents=True, exist_ok=True)
        (outdir / "radlex" / "index.html").write_text(html, encoding="utf-8")
    return saved

def download_recist11(outdir: Path):
    rr = S.get(RECIST11_PDF, stream=True, timeout=180)
    if rr.status_code == 200:
        dest = outdir / "guidelines" / "RECIST_1.1_EORTC.pdf"
        save_stream(rr, dest)
        return dest

def download_irecist(outdir: Path):
    """
    Download iRECIST PDF from PMC.
    Falls back to creating download instructions if direct download fails.
    """
    dest_dir = outdir / "guidelines"
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest_pdf = dest_dir / "iRECIST.pdf"
    
    # Try direct PDF download from PMC
    pdf_url = f"https://pmc.ncbi.nlm.nih.gov/articles/{IRECIST_PMCID}/pdf/"
    try:
        rr = S.get(pdf_url, stream=True, timeout=180)
        if rr.status_code == 200:
            save_stream(rr, dest_pdf)
            return dest_pdf
    except Exception as e:
        print(f"  Warning: Direct download failed: {e}")
    
    # Fallback: save download instructions
    note_path = dest_dir / "iRECIST_DOWNLOAD_NOTE.txt"
    note_path.write_text(
        f"iRECIST Paper Download Instructions\n"
        f"{'=' * 50}\n\n"
        f"Automatic download failed. Manual download options:\n\n"
        f"1. PMC Article Page:\n"
        f"   https://www.ncbi.nlm.nih.gov/pmc/articles/{IRECIST_PMCID}/\n\n"
        f"2. Direct PDF Link:\n"
        f"   {pdf_url}\n\n"
        f"3. DOI: 10.1016/S1470-2045(17)30074-8\n\n"
        f"4. Citation:\n"
        f"   Seymour L, et al. iRECIST: guidelines for response criteria for\n"
        f"   use in trials testing immunotherapeutics. Lancet Oncol. 2017.\n",
        encoding="utf-8"
    )
    return note_path

def download_loinc(outdir: Path):
    """
    Download LOINC files or extract from manually downloaded zip.
    LOINC requires free registration, so manual download is recommended.
    """
    import zipfile
    
    loinc_dir = outdir / "loinc"
    loinc_dir.mkdir(parents=True, exist_ok=True)
    saved = []
    
    # Check for manually downloaded LOINC zip file
    loinc_zips = list(loinc_dir.glob("Loinc_*.zip")) + list(loinc_dir.glob("LOINC_*.zip"))
    
    if loinc_zips:
        # Extract Radiology Playbook from existing zip
        zip_path = loinc_zips[0]
        print(f"  Found existing LOINC zip: {zip_path.name}")
        try:
            with zipfile.ZipFile(zip_path, 'r') as zf:
                # Extract the Radiology Playbook CSV
                playbook_files = [f for f in zf.namelist() if 'RadiologyPlaybook' in f and f.endswith('.csv')]
                for pf in playbook_files:
                    extract_path = loinc_dir / Path(pf).name
                    if not extract_path.exists():
                        zf.extract(pf, loinc_dir / "temp")
                        # Move from nested directory to loinc root
                        (loinc_dir / "temp" / pf).rename(extract_path)
                        saved.append(extract_path)
                        print(f"  Extracted: {extract_path.name}")
                
                # Clean up temp directory
                import shutil
                temp_dir = loinc_dir / "temp"
                if temp_dir.exists():
                    shutil.rmtree(temp_dir)
                    
                # Also extract README if present
                readme_files = [f for f in zf.namelist() if 'RadiologyPlaybook' in f and 'ReadMe' in f]
                for rf in readme_files:
                    extract_path = loinc_dir / Path(rf).name
                    if not extract_path.exists():
                        with zf.open(rf) as source, open(extract_path, 'wb') as target:
                            target.write(source.read())
                        saved.append(extract_path)
        except Exception as e:
            print(f"  Warning: Could not extract from zip: {e}")
    
    if saved:
        return saved
    
    # If no zip found, try to download (will likely require auth)
    try:
        rr = S.get(LOINC_DOWNLOADS, timeout=60)
        rr.raise_for_status()
        (loinc_dir / "downloads_page.html").write_text(rr.text, encoding="utf-8")
        links = re.findall(r'href="(https?://loinc\.org/[^"]+\.(?:zip|csv))"', rr.text, re.I)
        links = list(dict.fromkeys(links))
        
        for url in links:
            fn = url.split("/")[-1]
            dest = loinc_dir / fn
            r2 = S.get(url, stream=True, timeout=300)
            if r2.status_code == 200:
                save_stream(r2, dest)
                saved.append(dest)
            elif r2.status_code == 403:
                break
    except Exception as e:
        print(f"  Note: Direct download failed (expected): {e}")
    
    # Create instructions for manual download
    if not saved:
        note_path = loinc_dir / "DOWNLOAD_INSTRUCTIONS.txt"
        note_path.write_text(
            "LOINC Radiology Playbook Download Instructions\n"
            "=" * 50 + "\n\n"
            "LOINC requires free registration to download files.\n\n"
            "Steps to download:\n"
            "1. Visit: https://loinc.org/downloads/\n"
            "2. Create free account or log in\n"
            "3. Download the complete LOINC package (e.g., Loinc_2.81.zip)\n"
            "4. Place the zip file in this directory (data/loinc/)\n"
            "5. Run this script again with --loinc flag\n\n"
            "The script will automatically extract:\n"
            "- LoincRsnaRadiologyPlaybook.csv\n"
            "- Related documentation\n\n"
            "Radiology Playbook User Guide:\n"
            "https://loinc.org/kb/users-guide/loinc-rsna-radiology-playbook-user-guide/\n",
            encoding="utf-8"
        )
        saved.append(note_path)
    
    return saved

# ----------------------------
# Workflows
# ----------------------------
def run_recommended_queries(outdir: Path, mindate: str, maxdate: str, all_records: bool,
                            page_size: int, elink_batch: int, rate: float):
    base = outdir / "pubmed_abstracts"
    base.mkdir(parents=True, exist_ok=True)
    
    all_abstracts = []
    for name, core_term in RECOMMENDED_QUERIES:
        term = f'{core_term} AND ("{mindate}"[PDAT]:"{maxdate}"[PDAT])'
        print(f"[PubMed] {name}")
        if all_records:
            pmids = esearch_all(term, mindate, maxdate, page_size=page_size, delay=rate)
        else:
            pmids = esearch_page(term, mindate, maxdate, 0, page_size)
        
        print(f"  PubMed IDs: {len(pmids)}")
        
        if pmids:
            print(f"  Fetching abstracts...")
            abstracts = fetch_abstracts(pmids, batch=elink_batch, delay=rate)
            print(f"  Abstracts retrieved: {len(abstracts)}")
            
            # Save abstracts for this query
            qdir = base / re.sub(r"[^A-Za-z0-9_.-]+", "_", name)
            qdir.mkdir(parents=True, exist_ok=True)
            
            # Save as JSON
            (qdir / "abstracts.json").write_text(
                json.dumps(abstracts, indent=2, ensure_ascii=False), 
                encoding="utf-8"
            )
            
            # Save as CSV for easy viewing
            if abstracts:
                import csv
                with open(qdir / "abstracts.csv", "w", newline="", encoding="utf-8") as f:
                    writer = csv.DictWriter(f, fieldnames=["pmid", "title", "journal", "year", "abstract", "authors"])
                    writer.writeheader()
                    for a in abstracts:
                        writer.writerow({
                            "pmid": a["pmid"],
                            "title": a["title"],
                            "journal": a["journal"],
                            "year": a["year"],
                            "abstract": a["abstract"],
                            "authors": "; ".join(a["authors"])
                        })
            
            all_abstracts.extend(abstracts)
        print()
    
    # Save combined file
    print(f"Total abstracts collected: {len(all_abstracts)}")
    (base / "all_abstracts.json").write_text(
        json.dumps(all_abstracts, indent=2, ensure_ascii=False),
        encoding="utf-8"
    )

def run_freeform_query(outdir: Path, query: str, mindate: str, maxdate: str, all_records: bool,
                       page_size: int, elink_batch: int, rate: float):
    base = outdir / "pubmed_abstracts" / "custom_query"
    base.mkdir(parents=True, exist_ok=True)
    
    print(f"[PubMed] Custom query")
    if all_records:
        pmids = esearch_all(query, mindate, maxdate, page_size=page_size, delay=rate)
    else:
        pmids = esearch_page(query, mindate, maxdate, 0, page_size)
    
    print(f"  PubMed IDs: {len(pmids)}")
    
    if pmids:
        print(f"  Fetching abstracts...")
        abstracts = fetch_abstracts(pmids, batch=elink_batch, delay=rate)
        print(f"  Abstracts retrieved: {len(abstracts)}")
        
        # Save abstracts
        (base / "abstracts.json").write_text(
            json.dumps(abstracts, indent=2, ensure_ascii=False),
            encoding="utf-8"
        )
        
        # Save as CSV
        if abstracts:
            import csv
            with open(base / "abstracts.csv", "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=["pmid", "title", "journal", "year", "abstract", "authors"])
                writer.writeheader()
                for a in abstracts:
                    writer.writerow({
                        "pmid": a["pmid"],
                        "title": a["title"],
                        "journal": a["journal"],
                        "year": a["year"],
                        "abstract": a["abstract"],
                        "authors": "; ".join(a["authors"])
                    })

# ----------------------------
# CLI
# ----------------------------
def parse_args():
    p = argparse.ArgumentParser(
        description="Fetch radiology/oncology corpora for RAG applications.",
        epilog="Example: python fetch_rad_corpora.py --all --start-date 2023-01-01 --end-date 2025-12-31"
    )
    p.add_argument("--outdir", default=str(OUTDIR), help="Output directory (default: data/)")
    p.add_argument("--rate", type=float, default=BASE_DELAY, help="Seconds between NCBI calls (default: based on API key)")
    p.add_argument("--page-size", type=int, default=1000, help="PubMed search page size")
    p.add_argument("--elink-batch", type=int, default=200, help="Batch size for abstract fetching")
    p.add_argument("--start-date", default="2023-01-01", help="Start date for literature search (YYYY-MM-DD)")
    p.add_argument("--end-date", default="2025-12-31", help="End date for literature search (YYYY-MM-DD)")
    p.add_argument("--all-records", action="store_true", help="Fetch all matching records (may take time)")
    
    # Data source toggles
    p.add_argument("--all", action="store_true", help="Download all data sources")
    p.add_argument("--pmc-queries", action="store_true", help="Fetch abstracts from top journals using predefined queries")
    p.add_argument("--pmc-query", default=None, help="Run a custom PubMed query (e.g., 'cancer AND imaging')")
    p.add_argument("--radlex", action="store_true", help="Download RadLex terminology")
    p.add_argument("--recist", action="store_true", help="Download RECIST 1.1 PDF")
    p.add_argument("--irecist", action="store_true", help="Download iRECIST PDF")
    p.add_argument("--loinc", action="store_true", help="Extract LOINC Radiology Playbook")
    return p.parse_args()

def main():
    args = parse_args()
    outdir = Path(args.outdir).resolve()
    outdir.mkdir(parents=True, exist_ok=True)

    if args.all:
        args.pmc_queries = True
        args.radlex = True
        args.recist = True
        args.irecist = True
        args.loinc = True
        args.all_records = True

    # PubMed/PMC workflows
    if args.pmc_queries:
        run_recommended_queries(outdir, args.start_date, args.end_date,
                                args.all_records, args.page_size, args.elink_batch, args.rate)

    if args.pmc_query:
        run_freeform_query(outdir, args.pmc_query, args.start_date, args.end_date,
                           args.all_records, args.page_size, args.elink_batch, args.rate)

    # Others
    if args.radlex:
        files = download_radlex(outdir)
        print(f"[RadLex] files: {len(files)}")

    if args.recist:
        p = download_recist11(outdir)
        print(f"[RECIST 1.1] {p if p else 'failed'}")

    if args.irecist:
        p = download_irecist(outdir)
        if p and str(p).endswith("_NOTE.txt"):
            print(f"[iRECIST] Manual download required. Instructions saved: {p.name}")
        elif p:
            print(f"[iRECIST] Successfully downloaded: {p.name}")
        else:
            print(f"[iRECIST] Failed to download")

    if args.loinc:
        print("[LOINC] Processing...")
        files = download_loinc(outdir)
        csv_files = [f for f in files if str(f).endswith('.csv')]
        if csv_files:
            print(f"[LOINC] Successfully extracted {len(csv_files)} CSV file(s):")
            for f in csv_files:
                print(f"  - {f.name}")
        elif any(str(f).endswith('INSTRUCTIONS.txt') for f in files):
            print(f"[LOINC] Created download instructions (manual download required)")
        else:
            print(f"[LOINC] Processed {len(files)} file(s)")

if __name__ == "__main__":
    main()
