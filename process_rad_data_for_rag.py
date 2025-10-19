#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Unified radiology data processor for RAG systems.

Processes both RadLex and LOINC data into RAG-optimized formats:
- RadLex: Terminology ontology (22K+ terms)
- LOINC: Radiology procedures (7K+ codes with enriched synonyms)

Output: JSON documents and text chunks ready for vector databases.
"""

import json
import csv
import zipfile
import re
from pathlib import Path
from xml.etree import ElementTree as ET
from collections import defaultdict

#############################################################################
# RADLEX PROCESSING
#############################################################################

# Namespaces in RadLex OWL file
NS = {
    'rdf': 'http://www.w3.org/1999/02/22-rdf-syntax-ns#',
    'rdfs': 'http://www.w3.org/2000/01/rdf-schema#',
    'owl': 'http://www.w3.org/2002/07/owl#',
    'RID': 'http://www.radlex.org/RID/',
    'obo': 'http://www.geneontology.org/formats/oboInOwl#'
}

def extract_rid(uri):
    """Extract RID from URI"""
    if not uri:
        return None
    match = re.search(r'RID(\d+)', uri)
    return f"RID{match.group(1)}" if match else None

def parse_radlex_owl(owl_file):
    """Parse RadLex OWL file and extract all terms."""
    print(f"  Parsing {owl_file.name}...")
    tree = ET.parse(owl_file)
    root = tree.getroot()
    
    terms = {}
    
    for cls in root.findall('.//owl:Class', NS):
        about = cls.get(f"{{{NS['rdf']}}}about")
        if not about or 'RID' not in about:
            continue
        
        rid = extract_rid(about)
        if not rid:
            continue
        
        term = {
            'rid': rid,
            'uri': about,
            'preferred_name': None,
            'definition': None,
            'synonyms': [],
            'acronyms': [],
            'parent_classes': [],
            'umls_cui': [],
            'snomed_id': [],
            'fma_id': None,
            'source': None,
        }
        
        # Extract label
        label = cls.find('rdfs:label', NS)
        if label is not None and label.text:
            term['label'] = label.text
        
        # Extract preferred name
        for pref in cls.findall('.//RID:Preferred_name[@{http://www.w3.org/XML/1998/namespace}lang="en"]', NS):
            if pref.text:
                term['preferred_name'] = pref.text
                break
        
        if not term['preferred_name']:
            pref = cls.find('.//RID:Preferred_name', NS)
            if pref is not None and pref.text:
                term['preferred_name'] = pref.text
        
        # Definition
        definition = cls.find('.//RID:Definition', NS)
        if definition is not None and definition.text:
            term['definition'] = definition.text
        
        # Synonyms
        for syn in cls.findall('.//RID:Synonym', NS):
            if syn.text and syn.text not in term['synonyms']:
                term['synonyms'].append(syn.text)
        
        # Acronyms
        for acr in cls.findall('.//RID:Acronym', NS):
            if acr.text and acr.text not in term['acronyms']:
                term['acronyms'].append(acr.text)
        
        # Parent classes
        for parent in cls.findall('.//rdfs:subClassOf', NS):
            parent_uri = parent.get(f"{{{NS['rdf']}}}resource")
            if parent_uri:
                parent_rid = extract_rid(parent_uri)
                if parent_rid and parent_rid not in term['parent_classes']:
                    term['parent_classes'].append(parent_rid)
        
        # UMLS CUI
        for umls in cls.findall('.//RID:UMLS_CUI', NS):
            if umls.text and umls.text not in term['umls_cui']:
                term['umls_cui'].append(umls.text)
        
        # SNOMED ID
        for snomed in cls.findall('.//RID:SNOMED_CT_ID', NS):
            if snomed.text and snomed.text not in term['snomed_id']:
                term['snomed_id'].append(snomed.text)
        
        # FMA ID
        fma = cls.find('.//RID:FMAID', NS)
        if fma is not None and fma.text:
            term['fma_id'] = fma.text
        
        # Source
        source = cls.find('.//RID:Source', NS)
        if source is not None and source.text:
            term['source'] = source.text
        
        if term['preferred_name'] or term.get('label'):
            terms[rid] = term
    
    print(f"  Extracted {len(terms):,} RadLex terms")
    return terms

def create_radlex_rag_documents(terms):
    """Convert RadLex terms into RAG-optimized documents."""
    documents = []
    
    for rid, term in terms.items():
        text_parts = []
        
        name = term['preferred_name'] or term.get('label', rid)
        text_parts.append(f"Term: {name}")
        text_parts.append(f"RadLex ID: {rid}")
        
        if term['definition']:
            text_parts.append(f"Definition: {term['definition']}")
        
        if term['synonyms']:
            text_parts.append(f"Synonyms: {', '.join(term['synonyms'])}")
        
        if term['acronyms']:
            text_parts.append(f"Acronyms: {', '.join(term['acronyms'])}")
        
        doc = {
            'id': rid,
            'name': name,
            'text': '\n'.join(text_parts),
            'metadata': {
                'rid': rid,
                'type': 'radlex_term',
                'preferred_name': term['preferred_name'],
                'synonyms': term['synonyms'],
                'acronyms': term['acronyms'],
                'definition': term['definition'],
                'parent_classes': term['parent_classes'],
                'umls_cui': term['umls_cui'],
                'snomed_id': term['snomed_id'],
                'fma_id': term['fma_id'],
                'source': term['source'],
            }
        }
        
        documents.append(doc)
    
    return documents

def create_radlex_text_chunks(terms, max_chunk_size=500):
    """Create text chunks for RadLex terms."""
    chunks = []
    
    for rid, term in terms.items():
        name = term['preferred_name'] or term.get('label', rid)
        chunk_text = f"{name} ({rid})"
        
        if term['definition']:
            chunk_text += f"\n{term['definition']}"
        
        if term['synonyms']:
            syn_text = f"\nAlso known as: {', '.join(term['synonyms'][:5])}"
            if len(chunk_text) + len(syn_text) < max_chunk_size:
                chunk_text += syn_text
        
        if term['acronyms']:
            acr_text = f"\nAcronyms: {', '.join(term['acronyms'])}"
            if len(chunk_text) + len(acr_text) < max_chunk_size:
                chunk_text += acr_text
        
        chunks.append({
            'text': chunk_text,
            'metadata': {
                'rid': rid,
                'name': name,
                'type': 'radlex_term'
            }
        })
    
    return chunks

#############################################################################
# LOINC PROCESSING (ENRICHED WITH SYNONYMS)
#############################################################################

def parse_loinc_playbook(csv_file):
    """Parse LOINC Radiology Playbook CSV."""
    print(f"  Parsing {csv_file.name}...")
    
    procedures = defaultdict(lambda: {
        'loinc_number': None,
        'long_common_name': None,
        'components': [],
        'modality': [],
        'modality_subtype': [],
        'anatomic_location': [],
        'imaging_focus': [],
        'laterality': [],
        'view': [],
        'timing': [],
        'maneuver': [],
        'pharmaceutical': [],
        'reason_for_exam': [],
        'guidance': [],
        'subject': [],
        'radlex_ids': set(),
    })
    
    with open(csv_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        
        for row in reader:
            loinc_num = row['LoincNumber']
            
            if procedures[loinc_num]['loinc_number'] is None:
                procedures[loinc_num]['loinc_number'] = loinc_num
                procedures[loinc_num]['long_common_name'] = row['LongCommonName']
            
            component = {
                'part_number': row['PartNumber'],
                'part_type': row['PartTypeName'],
                'part_name': row['PartName'],
                'sequence_order': row['PartSequenceOrder'],
                'rid': row['RID'] if row['RID'] else None,
                'preferred_name': row['PreferredName'] if row['PreferredName'] else None,
            }
            procedures[loinc_num]['components'].append(component)
            
            part_type = row['PartTypeName']
            part_name = row['PartName']
            
            if 'Modality' in part_type and part_name:
                if 'Subtype' in part_type:
                    procedures[loinc_num]['modality_subtype'].append(part_name)
                else:
                    procedures[loinc_num]['modality'].append(part_name)
            elif 'Anatomic' in part_type and part_name:
                if 'Focus' in part_type:
                    procedures[loinc_num]['imaging_focus'].append(part_name)
                else:
                    procedures[loinc_num]['anatomic_location'].append(part_name)
            elif 'Laterality' in part_type and part_name:
                procedures[loinc_num]['laterality'].append(part_name)
            elif 'View' in part_type and part_name:
                procedures[loinc_num]['view'].append(part_name)
            elif 'Timing' in part_type and part_name:
                procedures[loinc_num]['timing'].append(part_name)
            elif 'Maneuver' in part_type and part_name:
                procedures[loinc_num]['maneuver'].append(part_name)
            elif 'Pharmaceutical' in part_type and part_name:
                procedures[loinc_num]['pharmaceutical'].append(part_name)
            elif 'Reason' in part_type and part_name:
                procedures[loinc_num]['reason_for_exam'].append(part_name)
            elif 'Guidance' in part_type and part_name:
                procedures[loinc_num]['guidance'].append(part_name)
            elif 'Subject' in part_type and part_name:
                procedures[loinc_num]['subject'].append(part_name)
            
            if row['RID']:
                procedures[loinc_num]['radlex_ids'].add(row['RID'])
    
    # Convert sets to lists
    for proc in procedures.values():
        proc['radlex_ids'] = sorted(list(proc['radlex_ids']))
        for key in ['modality', 'modality_subtype', 'anatomic_location', 'imaging_focus',
                   'laterality', 'view', 'timing', 'maneuver', 'pharmaceutical',
                   'reason_for_exam', 'guidance', 'subject']:
            proc[key] = list(dict.fromkeys(proc[key]))
    
    procedures_dict = dict(procedures)
    print(f"  Extracted {len(procedures_dict):,} procedures")
    return procedures_dict

def extract_loinc_enrichment_data(zip_path):
    """Extract enrichment data from main LOINC table."""
    print(f"  Extracting enrichment data from {zip_path.name}...")
    
    loinc_enrichment = {}
    
    with zipfile.ZipFile(zip_path, 'r') as zf:
        loinc_file = None
        for name in zf.namelist():
            if name.endswith('LoincTable/Loinc.csv') or name.endswith('/Loinc.csv'):
                loinc_file = name
                break
        
        if not loinc_file:
            print("  Warning: Main Loinc.csv not found")
            return loinc_enrichment
        
        with zf.open(loinc_file) as f:
            reader = csv.DictReader(line.decode('utf-8') for line in f)
            
            count = 0
            for row in reader:
                loinc_num = row.get('LOINC_NUM', '')
                if not loinc_num:
                    continue
                
                related_names = row.get('RELATEDNAMES2', '').strip()
                synonyms = []
                if related_names:
                    synonyms = [s.strip() for s in related_names.split(';') if s.strip()]
                
                assoc_obs = row.get('AssociatedObservations', '').strip()
                associated_codes = []
                if assoc_obs:
                    associated_codes = [s.strip() for s in assoc_obs.split(';') if s.strip()]
                
                loinc_enrichment[loinc_num] = {
                    'short_name': row.get('SHORTNAME', '').strip(),
                    'consumer_name': row.get('CONSUMER_NAME', '').strip(),
                    'display_name': row.get('DisplayName', '').strip(),
                    'synonyms': synonyms,
                    'loinc_class': row.get('CLASS', '').strip(),
                    'order_obs': row.get('ORDER_OBS', '').strip(),
                    'associated_observations': associated_codes,
                }
                
                count += 1
                if count % 10000 == 0:
                    print(f"    Processed {count:,} codes...")
    
    print(f"  Loaded {len(loinc_enrichment):,} enrichments")
    return loinc_enrichment

def enrich_loinc_procedures(procedures, enrichment):
    """Enrich procedures with synonym data."""
    enriched_count = 0
    for loinc_num, proc in procedures.items():
        if loinc_num in enrichment:
            enrich = enrichment[loinc_num]
            proc['short_name'] = enrich['short_name']
            proc['consumer_name'] = enrich['consumer_name']
            proc['display_name'] = enrich['display_name']
            proc['synonyms'] = enrich['synonyms']
            proc['loinc_class'] = enrich['loinc_class']
            proc['order_obs'] = enrich['order_obs']
            proc['associated_observations'] = enrich['associated_observations']
            enriched_count += 1
        else:
            proc['short_name'] = ''
            proc['consumer_name'] = ''
            proc['display_name'] = ''
            proc['synonyms'] = []
            proc['loinc_class'] = ''
            proc['order_obs'] = ''
            proc['associated_observations'] = []
    
    print(f"  Enriched {enriched_count:,}/{len(procedures):,} procedures")
    return procedures

def create_loinc_rag_documents(procedures):
    """Convert LOINC procedures into RAG-optimized documents."""
    documents = []
    
    for loinc_num, proc in procedures.items():
        text_parts = []
        
        text_parts.append(f"Procedure: {proc['long_common_name']}")
        text_parts.append(f"LOINC Code: {loinc_num}")
        
        if proc.get('short_name'):
            text_parts.append(f"Short Name: {proc['short_name']}")
        
        if proc.get('consumer_name') and proc['consumer_name'] != proc['long_common_name']:
            text_parts.append(f"Consumer Name: {proc['consumer_name']}")
        
        if proc['modality']:
            modality_text = ', '.join(proc['modality'])
            if proc['modality_subtype']:
                modality_text += f" ({', '.join(proc['modality_subtype'])})"
            text_parts.append(f"Modality: {modality_text}")
        
        if proc['anatomic_location']:
            anatomy = ', '.join(proc['anatomic_location'])
            if proc['imaging_focus']:
                anatomy += f" - {', '.join(proc['imaging_focus'])}"
            if proc['laterality']:
                anatomy += f" [{', '.join(proc['laterality'])}]"
            text_parts.append(f"Anatomy: {anatomy}")
        
        if proc['view']:
            text_parts.append(f"View: {', '.join(proc['view'])}")
        
        if proc['timing']:
            text_parts.append(f"Timing: {', '.join(proc['timing'])}")
        
        if proc['pharmaceutical']:
            text_parts.append(f"Agent: {', '.join(proc['pharmaceutical'])}")
        
        if proc['maneuver']:
            text_parts.append(f"Maneuver: {', '.join(proc['maneuver'])}")
        
        if proc['guidance']:
            text_parts.append(f"Guidance: {', '.join(proc['guidance'])}")
        
        if proc['reason_for_exam']:
            text_parts.append(f"Indication: {', '.join(proc['reason_for_exam'])}")
        
        if proc.get('synonyms'):
            syn_list = proc['synonyms'][:10]
            if syn_list:
                text_parts.append(f"Also known as: {', '.join(syn_list)}")
        
        doc = {
            'id': loinc_num,
            'name': proc['long_common_name'],
            'text': '\n'.join(text_parts),
            'metadata': {
                'loinc_number': loinc_num,
                'type': 'loinc_radiology_procedure',
                'long_common_name': proc['long_common_name'],
                'short_name': proc.get('short_name', ''),
                'consumer_name': proc.get('consumer_name', ''),
                'synonyms': proc.get('synonyms', []),
                'modality': proc['modality'],
                'modality_subtype': proc['modality_subtype'],
                'anatomic_location': proc['anatomic_location'],
                'imaging_focus': proc['imaging_focus'],
                'laterality': proc['laterality'],
                'view': proc['view'],
                'timing': proc['timing'],
                'pharmaceutical': proc['pharmaceutical'],
                'maneuver': proc['maneuver'],
                'guidance': proc['guidance'],
                'reason_for_exam': proc['reason_for_exam'],
                'subject': proc['subject'],
                'radlex_ids': proc['radlex_ids'],
                'loinc_class': proc.get('loinc_class', ''),
                'associated_observations': proc.get('associated_observations', []),
                'component_count': len(proc['components']),
                'synonym_count': len(proc.get('synonyms', [])),
            }
        }
        
        documents.append(doc)
    
    return documents

def create_loinc_text_chunks(procedures, max_chunk_size=600):
    """Create text chunks for LOINC procedures."""
    chunks = []
    
    for loinc_num, proc in procedures.items():
        name = proc['long_common_name']
        chunk_text = f"{name} (LOINC: {loinc_num})"
        
        if proc.get('short_name') and proc['short_name'] != name:
            add_text = f"\nShort name: {proc['short_name']}"
            if len(chunk_text) + len(add_text) < max_chunk_size:
                chunk_text += add_text
        
        if proc['modality']:
            add_text = f"\nModality: {', '.join(proc['modality'])}"
            if len(chunk_text) + len(add_text) < max_chunk_size:
                chunk_text += add_text
        
        if proc['anatomic_location']:
            add_text = f"\nAnatomy: {', '.join(proc['anatomic_location'])}"
            if len(chunk_text) + len(add_text) < max_chunk_size:
                chunk_text += add_text
        
        if proc['imaging_focus']:
            add_text = f"\nFocus: {', '.join(proc['imaging_focus'][:3])}"
            if len(chunk_text) + len(add_text) < max_chunk_size:
                chunk_text += add_text
        
        if proc.get('synonyms'):
            add_text = f"\nSynonyms: {', '.join(proc['synonyms'][:5])}"
            if len(chunk_text) + len(add_text) < max_chunk_size:
                chunk_text += add_text
        
        chunks.append({
            'text': chunk_text,
            'metadata': {
                'loinc_number': loinc_num,
                'name': name,
                'type': 'loinc_radiology_procedure'
            }
        })
    
    return chunks

#############################################################################
# COMMON UTILITIES
#############################################################################

def save_json(data, filepath):
    """Save data as JSON"""
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    print(f"    Saved: {filepath.name}")

def save_csv(data, fieldnames, filepath):
    """Save data as CSV"""
    with open(filepath, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)
    print(f"    Saved: {filepath.name}")

#############################################################################
# MAIN PROCESSING
#############################################################################

def process_radlex(data_dir):
    """Process RadLex data."""
    print("\n" + "="*70)
    print("PROCESSING RADLEX")
    print("="*70)
    
    owl_file = data_dir / 'radlex' / 'RadLex.owl'
    output_dir = data_dir / 'radlex' / 'processed'
    output_dir.mkdir(exist_ok=True)
    
    # Parse
    terms = parse_radlex_owl(owl_file)
    
    # Create RAG formats
    print("  Creating RAG documents...")
    rag_docs = create_radlex_rag_documents(terms)
    
    print("  Creating text chunks...")
    chunks = create_radlex_text_chunks(terms)
    
    # Statistics
    stats = {
        'total_terms': len(terms),
        'terms_with_definition': sum(1 for t in terms.values() if t['definition']),
        'terms_with_synonyms': sum(1 for t in terms.values() if t['synonyms']),
        'total_synonyms': sum(len(t['synonyms']) for t in terms.values()),
    }
    
    # Save
    print("  Saving files...")
    save_json(terms, output_dir / 'radlex_terms_full.json')
    save_json(rag_docs, output_dir / 'radlex_rag_documents.json')
    save_json(chunks, output_dir / 'radlex_text_chunks.json')
    save_json(stats, output_dir / 'radlex_statistics.json')
    
    # CSV
    csv_data = []
    for rid, term in terms.items():
        csv_data.append({
            'rid': rid,
            'preferred_name': term['preferred_name'] or '',
            'definition': term['definition'] or '',
            'synonyms': '; '.join(term['synonyms']) if term['synonyms'] else '',
            'acronyms': '; '.join(term['acronyms']) if term['acronyms'] else '',
        })
    save_csv(csv_data, ['rid', 'preferred_name', 'definition', 'synonyms', 'acronyms'],
             output_dir / 'radlex_terms.csv')
    
    print(f"\n  ✅ RadLex: {len(terms):,} terms → {len(rag_docs):,} documents")
    return stats

def process_loinc(data_dir):
    """Process LOINC data with enrichment."""
    print("\n" + "="*70)
    print("PROCESSING LOINC (ENRICHED)")
    print("="*70)
    
    csv_file = data_dir / 'loinc' / 'LoincRsnaRadiologyPlaybook.csv'
    zip_file = data_dir / 'loinc' / 'Loinc_2.81.zip'
    output_dir = data_dir / 'loinc' / 'processed'
    output_dir.mkdir(exist_ok=True)
    
    # Parse Playbook
    procedures = parse_loinc_playbook(csv_file)
    
    # Enrich with main LOINC table
    enrichment = extract_loinc_enrichment_data(zip_file)
    procedures = enrich_loinc_procedures(procedures, enrichment)
    
    # Create RAG formats
    print("  Creating RAG documents...")
    rag_docs = create_loinc_rag_documents(procedures)
    
    print("  Creating text chunks...")
    chunks = create_loinc_text_chunks(procedures)
    
    # Statistics
    stats = {
        'total_procedures': len(procedures),
        'procedures_with_synonyms': sum(1 for p in procedures.values() if p.get('synonyms')),
        'total_synonyms': sum(len(p.get('synonyms', [])) for p in procedures.values()),
        'avg_synonyms': round(sum(len(p.get('synonyms', [])) for p in procedures.values()) / len(procedures), 1),
        'procedures_with_radlex': sum(1 for p in procedures.values() if p['radlex_ids']),
    }
    
    # Save
    print("  Saving files...")
    save_json(procedures, output_dir / 'loinc_procedures_full.json')
    save_json(rag_docs, output_dir / 'loinc_rag_documents.json')
    save_json(chunks, output_dir / 'loinc_text_chunks.json')
    save_json(stats, output_dir / 'loinc_statistics.json')
    
    # CSV
    csv_data = []
    for loinc_num, proc in procedures.items():
        csv_data.append({
            'loinc_number': loinc_num,
            'long_common_name': proc['long_common_name'],
            'short_name': proc.get('short_name', ''),
            'modality': '; '.join(proc['modality']) if proc['modality'] else '',
            'anatomic_location': '; '.join(proc['anatomic_location']) if proc['anatomic_location'] else '',
            'synonyms_count': len(proc.get('synonyms', [])),
            'top_synonyms': '; '.join(proc.get('synonyms', [])[:5]) if proc.get('synonyms') else '',
        })
    save_csv(csv_data, ['loinc_number', 'long_common_name', 'short_name', 'modality', 
                        'anatomic_location', 'synonyms_count', 'top_synonyms'],
             output_dir / 'loinc_procedures.csv')
    
    print(f"\n  ✅ LOINC: {len(procedures):,} procedures → {len(rag_docs):,} documents")
    print(f"     Enriched with {stats['total_synonyms']:,} synonyms (avg {stats['avg_synonyms']} per procedure)")
    return stats

def main():
    """Main processing function."""
    print("\n" + "="*70)
    print("RADIOLOGY DATA PROCESSOR FOR RAG")
    print("="*70)
    print("\nProcessing RadLex and LOINC data into RAG-optimized formats...")
    
    data_dir = Path('data')
    
    # Process both datasets
    radlex_stats = process_radlex(data_dir)
    loinc_stats = process_loinc(data_dir)
    
    # Summary
    total_docs = radlex_stats['total_terms'] + loinc_stats['total_procedures']
    total_synonyms = radlex_stats['total_synonyms'] + loinc_stats['total_synonyms']
    
    print("\n" + "="*70)
    print("PROCESSING COMPLETE")
    print("="*70)
    print(f"\n📊 Total Documents: {total_docs:,}")
    print(f"   - RadLex terms: {radlex_stats['total_terms']:,}")
    print(f"   - LOINC procedures: {loinc_stats['total_procedures']:,}")
    print(f"\n🔍 Total Synonyms: {total_synonyms:,}")
    print(f"\n📁 Output Locations:")
    print(f"   - data/radlex/processed/")
    print(f"   - data/loinc/processed/")
    print(f"\n🚀 Ready for RAG ingestion!")
    print(f"   Use: *_rag_documents.json or *_text_chunks.json")

if __name__ == '__main__':
    main()



