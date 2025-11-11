#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Process Radiology Terminology from Zip Archives

Extracts and processes LOINC and RadLex data from zip files, preserving
hierarchical structure and relationships for optimal RAG retrieval.

Usage:
    python process_terminology.py --process loinc
    python process_terminology.py --process radlex
    python process_terminology.py --process-all
"""

import json
import csv
import zipfile
import argparse
from pathlib import Path
from typing import List, Dict
import xml.etree.ElementTree as ET


class LOINCProcessor:
    """Process LOINC Radiology Playbook from zip archive"""
    
    def __init__(self, data_dir: Path = Path("data/loinc")):
        self.data_dir = data_dir
        self.output_dir = data_dir / "processed"
        self.output_dir.mkdir(parents=True, exist_ok=True)
    
    def process(self):
        """Extract and process LOINC data"""
        print("\n" + "="*70)
        print("PROCESSING LOINC RADIOLOGY PLAYBOOK")
        print("="*70)
        
        # Find zip file
        zip_files = list(self.data_dir.glob("Loinc_*.zip")) + list(self.data_dir.glob("LOINC_*.zip"))
        if not zip_files:
            print("❌ No LOINC zip file found")
            return
        
        zip_path = zip_files[0]
        print(f"\n📦 Extracting from: {zip_path.name}")
        
        # Extract Radiology Playbook CSV
        procedures = []
        with zipfile.ZipFile(zip_path, 'r') as zf:
            playbook_files = [f for f in zf.namelist() 
                            if 'RadiologyPlaybook' in f and f.endswith('.csv')]
            
            if not playbook_files:
                print("❌ RadiologyPlaybook.csv not found in zip")
                return
            
            csv_name = playbook_files[0]
            print(f"📄 Found: {Path(csv_name).name}")
            
            # Read CSV from zip
            with zf.open(csv_name) as csv_file:
                # Decode bytes to text
                text_file = (line.decode('utf-8') for line in csv_file)
                reader = csv.DictReader(text_file)
                
                for row in reader:
                    procedures.append(row)
        
        print(f"✅ Extracted {len(procedures):,} procedures")
        
        # Create RAG documents
        print("\n📝 Creating RAG documents...")
        documents = self._create_rag_documents(procedures)
        
        # Save full data
        full_output = self.output_dir / "loinc_procedures_full.json"
        with open(full_output, 'w', encoding='utf-8') as f:
            json.dump(procedures, f, indent=2, ensure_ascii=False)
        print(f"✅ Saved full data: {full_output.name}")
        
        # Save RAG documents
        rag_output = self.output_dir / "loinc_rag_documents.json"
        with open(rag_output, 'w', encoding='utf-8') as f:
            json.dump(documents, f, indent=2, ensure_ascii=False)
        print(f"✅ Saved RAG documents: {rag_output.name}")
        
        # Save statistics
        stats = self._calculate_statistics(procedures, documents)
        stats_output = self.output_dir / "loinc_statistics.json"
        with open(stats_output, 'w', encoding='utf-8') as f:
            json.dump(stats, f, indent=2)
        print(f"✅ Saved statistics: {stats_output.name}")
        
        # Print statistics
        print(f"\n📊 Statistics:")
        print(f"   Raw CSV records: {stats['total_raw_records']:,}")
        print(f"   Unique procedures: {stats['total_unique_procedures']:,}")
        print(f"   Unique anatomical parts: {stats['unique_anatomical_parts']}")
        print(f"   Unique RadLex mappings: {stats['unique_radlex_mappings']}")
        print(f"   Unique component types: {stats['unique_part_types']}")
        
        return documents
    
    def _create_rag_documents(self, procedures: List[Dict]) -> List[Dict]:
        """
        Create RAG-optimized documents from LOINC procedures
        
        Each procedure becomes a rich document with:
        - Full procedure description
        - LOINC code and metadata
        - Anatomical and modality context via RadLex mapping
        """
        documents = []
        
        # Group by LOINC number to aggregate parts
        from collections import defaultdict
        loinc_groups = defaultdict(list)
        
        for proc in procedures:
            loinc_num = proc.get('LoincNumber', '')
            if loinc_num:
                loinc_groups[loinc_num].append(proc)
        
        # Create document for each unique LOINC code
        for loinc_num, parts in loinc_groups.items():
            # Use first part for main info
            main = parts[0]
            
            # Build rich text description
            text_parts = []
            
            # Main procedure name
            if main.get('LongCommonName'):
                text_parts.append(f"Procedure: {main['LongCommonName']}")
            
            text_parts.append(f"LOINC Code: {loinc_num}")
            
            # Aggregate anatomical parts
            anatomies = set()
            radlex_ids = set()
            part_types = set()
            
            for part in parts:
                if part.get('PartName'):
                    anatomies.add(part['PartName'])
                if part.get('RID'):
                    radlex_ids.add(part['RID'])
                if part.get('PartTypeName'):
                    part_types.add(part['PartTypeName'])
            
            # Add anatomical info
            if anatomies:
                text_parts.append(f"Anatomical Regions: {', '.join(sorted(anatomies))}")
            
            if radlex_ids:
                text_parts.append(f"RadLex IDs: {', '.join(sorted(radlex_ids))}")
            
            if part_types:
                # Clean up part types for readability
                clean_types = [pt.replace('Rad.', '').replace('.', ' ') for pt in part_types]
                text_parts.append(f"Components: {', '.join(sorted(clean_types))}")
            
            # Create document
            doc = {
                'id': f"LOINC_{loinc_num.replace('-', '_')}",
                'text': '\n'.join(text_parts),
                'metadata': {
                    'source': 'LOINC_Radiology_Playbook',
                    'type': 'radiology_procedure',
                    'loinc_code': loinc_num,
                    'procedure_name': main.get('LongCommonName', ''),
                    'anatomical_parts': list(anatomies),
                    'radlex_ids': list(radlex_ids),
                    'part_count': len(parts),
                    'char_count': len('\n'.join(text_parts))
                }
            }
            
            documents.append(doc)
        
        return documents
    
    def _calculate_statistics(self, procedures: List[Dict], documents: List[Dict]) -> Dict:
        """Calculate statistics about the LOINC data"""
        # Get unique part names and RadLex IDs
        part_names = set(p.get('PartName', '') for p in procedures if p.get('PartName'))
        radlex_ids = set(p.get('RID', '') for p in procedures if p.get('RID'))
        part_types = set(p.get('PartTypeName', '') for p in procedures if p.get('PartTypeName'))
        
        text_lengths = [len(d['text']) for d in documents]
        
        return {
            'total_raw_records': len(procedures),
            'total_unique_procedures': len(documents),
            'unique_anatomical_parts': len(part_names),
            'unique_radlex_mappings': len(radlex_ids),
            'unique_part_types': len(part_types),
            'avg_text_length': sum(text_lengths) / len(text_lengths) if text_lengths else 0,
            'min_text_length': min(text_lengths) if text_lengths else 0,
            'max_text_length': max(text_lengths) if text_lengths else 0
        }


class RadLexProcessor:
    """Process RadLex ontology from zip archive"""
    
    def __init__(self, data_dir: Path = Path("data/radlex")):
        self.data_dir = data_dir
        self.output_dir = data_dir / "processed"
        self.output_dir.mkdir(parents=True, exist_ok=True)
    
    def process(self):
        """Extract and process RadLex ontology"""
        print("\n" + "="*70)
        print("PROCESSING RADLEX ONTOLOGY")
        print("="*70)
        
        # Find OWL zip file
        owl_zips = list(self.data_dir.glob("RadLex*OWL*.zip"))
        if not owl_zips:
            print("❌ No RadLex OWL zip file found")
            return
        
        zip_path = owl_zips[0]
        print(f"\n📦 Extracting from: {zip_path.name}")
        
        # Extract OWL file
        owl_content = None
        with zipfile.ZipFile(zip_path, 'r') as zf:
            owl_files = [f for f in zf.namelist() if f.endswith('.owl')]
            
            if not owl_files:
                print("❌ .owl file not found in zip")
                return
            
            owl_name = owl_files[0]
            print(f"📄 Found: {Path(owl_name).name}")
            
            # Read OWL content
            with zf.open(owl_name) as owl_file:
                owl_content = owl_file.read()
        
        print(f"✅ Extracted OWL file ({len(owl_content):,} bytes)")
        
        # Parse OWL
        print("\n🔍 Parsing OWL ontology...")
        terms = self._parse_owl(owl_content)
        print(f"✅ Parsed {len(terms):,} terms")
        
        # Create RAG documents
        print("\n📝 Creating RAG documents...")
        documents = self._create_rag_documents(terms)
        
        # Save full data
        full_output = self.output_dir / "radlex_terms_full.json"
        with open(full_output, 'w', encoding='utf-8') as f:
            json.dump(terms, f, indent=2, ensure_ascii=False)
        print(f"✅ Saved full data: {full_output.name}")
        
        # Save RAG documents
        rag_output = self.output_dir / "radlex_rag_documents.json"
        with open(rag_output, 'w', encoding='utf-8') as f:
            json.dump(documents, f, indent=2, ensure_ascii=False)
        print(f"✅ Saved RAG documents: {rag_output.name}")
        
        # Save statistics
        stats = self._calculate_statistics(terms, documents)
        stats_output = self.output_dir / "radlex_statistics.json"
        with open(stats_output, 'w', encoding='utf-8') as f:
            json.dump(stats, f, indent=2)
        print(f"✅ Saved statistics: {stats_output.name}")
        
        # Print statistics
        print(f"\n📊 Statistics:")
        print(f"   Total terms: {stats['total_terms']:,}")
        print(f"   Total documents: {stats['total_documents']:,}")
        print(f"   Terms with definitions: {stats['terms_with_definitions']:,}")
        print(f"   Terms with synonyms: {stats['terms_with_synonyms']:,}")
        
        return documents
    
    def _parse_owl(self, owl_content: bytes) -> List[Dict]:
        """
        Parse OWL file to extract RadLex terms with hierarchy
        
        Preserves:
        - Term IDs and labels
        - Definitions
        - Synonyms
        - Hierarchical relationships (subClassOf)
        """
        terms = []
        
        try:
            root = ET.fromstring(owl_content)
            
            # Define namespaces - including RadLex-specific namespaces
            namespaces = {
                'owl': 'http://www.w3.org/2002/07/owl#',
                'rdf': 'http://www.w3.org/1999/02/22-rdf-syntax-ns#',
                'rdfs': 'http://www.w3.org/2000/01/rdf-schema#',
                'skos': 'http://www.w3.org/2004/02/skos/core#',
                'obo': 'http://purl.obolibrary.org/obo/',
                'dc': 'http://purl.org/dc/elements/1.1/',
                'oboInOwl': 'http://www.geneontology.org/formats/oboInOwl#',
                'RID': 'http://www.radlex.org/RID/'  # RadLex-specific namespace
            }
            
            print("   Parsing OWL classes with improved namespace handling...")
            
            # Find all classes (terms) - try multiple patterns
            owl_classes = (
                root.findall('.//owl:Class', namespaces) or 
                root.findall('.//{http://www.w3.org/2002/07/owl#}Class')
            )
            
            print(f"   Found {len(owl_classes)} OWL classes")
            
            for owl_class in owl_classes:
                # Extract term ID/URI
                term_id = (
                    owl_class.get('{http://www.w3.org/1999/02/22-rdf-syntax-ns#}about') or
                    owl_class.get('rdf:about') or
                    ''
                )
                
                # Skip OWL/RDF system classes
                if not term_id or any(x in term_id for x in ['http://www.w3.org', 'http://purl.org/dc']):
                    continue
                
                # Extract RID
                rid = term_id.split('/')[-1] if '/' in term_id else term_id
                rid = rid.split('#')[-1] if '#' in rid else rid
                
                # Get label - try multiple approaches, prioritizing RadLex Preferred_name
                label = ''
                
                # FIRST: Try RadLex-specific Preferred_name (most descriptive)
                pref_name = owl_class.find('RID:Preferred_name', namespaces)
                if pref_name is not None and pref_name.text:
                    label = pref_name.text.strip()
                
                # Try without namespace prefix
                if not label:
                    for elem in owl_class:
                        if 'Preferred_name' in elem.tag:
                            if elem.text:
                                label = elem.text.strip()
                                break
                
                # Fallback to standard label fields
                if not label:
                    pref_label = owl_class.find('skos:prefLabel', namespaces)
                    if pref_label is not None and pref_label.text:
                        label = pref_label.text.strip()
                
                # Try rdfs:label (but this usually just has RID code for RadLex)
                if not label:
                    label_elem = owl_class.find('rdfs:label', namespaces)
                    if label_elem is not None and label_elem.text:
                        label_text = label_elem.text.strip()
                        # Only use if it's not just the RID
                        if label_text and not label_text.startswith('RID'):
                            label = label_text
                
                # Get definition - prioritize RadLex Definition field
                definition = ''
                
                # FIRST: Try RadLex-specific Definition
                def_elem = owl_class.find('RID:Definition', namespaces)
                if def_elem is not None and def_elem.text:
                    definition = def_elem.text.strip()
                
                # Try without namespace prefix
                if not definition:
                    for elem in owl_class:
                        if 'Definition' in elem.tag:
                            if elem.text:
                                definition = elem.text.strip()
                                break
                
                # Fallback to standard definition fields
                if not definition:
                    def_patterns = [
                        'skos:definition',
                        'obo:IAO_0000115',
                        'oboInOwl:hasDefinition'
                    ]
                    
                    for pattern in def_patterns:
                        def_elem = owl_class.find(f'.//{pattern}', namespaces)
                        if def_elem is not None and def_elem.text:
                            definition = def_elem.text.strip()
                            break
                
                # Get synonyms - prioritize RadLex Synonym field
                synonyms = []
                
                # FIRST: Try RadLex-specific Synonym
                for syn_elem in owl_class.findall('RID:Synonym', namespaces):
                    if syn_elem.text:
                        syn_text = syn_elem.text.strip()
                        if syn_text and syn_text not in synonyms:
                            synonyms.append(syn_text)
                
                # Also try Acronym as a synonym
                acronym_elem = owl_class.find('RID:Acronym', namespaces)
                if acronym_elem is not None and acronym_elem.text:
                    acronym = acronym_elem.text.strip()
                    if acronym and acronym not in synonyms and acronym != label:
                        synonyms.append(acronym)
                
                # Try without namespace prefix
                for elem in owl_class:
                    if 'Synonym' in elem.tag:
                        if elem.text:
                            syn_text = elem.text.strip()
                            if syn_text and syn_text not in synonyms:
                                synonyms.append(syn_text)
                
                # Fallback to standard synonym fields
                syn_patterns = [
                    'skos:altLabel',
                    'oboInOwl:hasExactSynonym',
                    'oboInOwl:hasSynonym'
                ]
                
                for pattern in syn_patterns:
                    for syn_elem in owl_class.findall(f'.//{pattern}', namespaces):
                        if syn_elem.text:
                            syn_text = syn_elem.text.strip()
                            if syn_text and syn_text not in synonyms:
                                synonyms.append(syn_text)
                
                # Get parent classes (hierarchy)
                parents = []
                for subclass_elem in owl_class.findall('.//rdfs:subClassOf', namespaces):
                    parent_uri = (
                        subclass_elem.get('{http://www.w3.org/1999/02/22-rdf-syntax-ns#}resource') or
                        subclass_elem.get('rdf:resource') or
                        ''
                    )
                    if parent_uri:
                        # Skip OWL system classes
                        if any(x in parent_uri for x in ['http://www.w3.org', 'http://purl.org/dc']):
                            continue
                        parent_rid = parent_uri.split('/')[-1]
                        parent_rid = parent_rid.split('#')[-1]
                        if parent_rid and parent_rid not in parents:
                            parents.append(parent_rid)
                
                # Also try direct namespace
                for subclass_elem in owl_class.findall('.//{http://www.w3.org/2000/01/rdf-schema#}subClassOf'):
                    parent_uri = subclass_elem.get('{http://www.w3.org/1999/02/22-rdf-syntax-ns#}resource', '')
                    if parent_uri:
                        if any(x in parent_uri for x in ['http://www.w3.org', 'http://purl.org/dc']):
                            continue
                        parent_rid = parent_uri.split('/')[-1]
                        parent_rid = parent_rid.split('#')[-1]
                        if parent_rid and parent_rid not in parents:
                            parents.append(parent_rid)
                
                # Only include terms with labels or valid RIDs
                if label or (rid and rid.startswith('RID')):
                    term = {
                        'rid': rid,
                        'label': label if label else rid,  # Use RID as fallback label
                        'definition': definition,
                        'synonyms': synonyms,
                        'parents': parents,
                        'uri': term_id
                    }
                    terms.append(term)
        
        except ET.ParseError as e:
            print(f"⚠️  Warning: XML parse error: {e}")
            print("   Attempting simplified parsing...")
            # Fallback: basic parsing
            terms = self._parse_owl_simple(owl_content)
        
        # Report parsing statistics
        terms_with_labels = sum(1 for t in terms if t['label'] != t['rid'])
        terms_with_defs = sum(1 for t in terms if t['definition'])
        terms_with_syns = sum(1 for t in terms if t['synonyms'])
        
        print(f"   ✓ Terms with descriptive labels: {terms_with_labels:,}")
        print(f"   ✓ Terms with definitions: {terms_with_defs:,}")
        print(f"   ✓ Terms with synonyms: {terms_with_syns:,}")
        
        return terms
    
    def _parse_owl_simple(self, owl_content: bytes) -> List[Dict]:
        """Simplified OWL parsing as fallback"""
        terms = []
        # Simple regex-based extraction
        import re
        
        content_str = owl_content.decode('utf-8', errors='ignore')
        
        # Find RDF IDs and labels
        rid_pattern = r'rdf:about="[^"]*/(RID\d+)"'
        label_pattern = r'<rdfs:label[^>]*>([^<]+)</rdfs:label>'
        
        rids = re.findall(rid_pattern, content_str)
        labels = re.findall(label_pattern, content_str)
        
        # Pair them up (rough approximation)
        for rid, label in zip(rids[:len(labels)], labels):
            terms.append({
                'rid': rid,
                'label': label.strip(),
                'definition': '',
                'synonyms': [],
                'parents': [],
                'uri': f'http://radlex.org/RID/{rid}'
            })
        
        return terms
    
    def _create_rag_documents(self, terms: List[Dict]) -> List[Dict]:
        """
        Create RAG-optimized documents from RadLex terms
        
        Each term becomes a rich document with:
        - Preferred label
        - Definition
        - Synonyms
        - Hierarchical context
        """
        documents = []
        
        for term in terms:
            # Build rich text representation
            text_parts = []
            
            # Main term
            text_parts.append(f"Term: {term['label']}")
            text_parts.append(f"RadLex ID: {term['rid']}")
            
            # Definition
            if term.get('definition'):
                text_parts.append(f"Definition: {term['definition']}")
            
            # Synonyms
            if term.get('synonyms'):
                text_parts.append(f"Synonyms: {', '.join(term['synonyms'])}")
            
            # Parents (hierarchy)
            if term.get('parents'):
                text_parts.append(f"Parent terms: {', '.join(term['parents'])}")
            
            # Create document
            doc = {
                'id': f"RadLex_{term['rid']}",
                'text': '\n'.join(text_parts),
                'metadata': {
                    'source': 'RadLex_Ontology',
                    'type': 'radiology_term',
                    'rid': term['rid'],
                    'label': term['label'],
                    'has_definition': bool(term.get('definition')),
                    'synonym_count': len(term.get('synonyms', [])),
                    'parent_count': len(term.get('parents', [])),
                    'char_count': len('\n'.join(text_parts))
                }
            }
            
            documents.append(doc)
        
        return documents
    
    def _calculate_statistics(self, terms: List[Dict], documents: List[Dict]) -> Dict:
        """Calculate statistics about RadLex data"""
        terms_with_def = sum(1 for t in terms if t.get('definition'))
        terms_with_syn = sum(1 for t in terms if t.get('synonyms'))
        
        text_lengths = [len(d['text']) for d in documents]
        
        return {
            'total_terms': len(terms),
            'total_documents': len(documents),
            'terms_with_definitions': terms_with_def,
            'terms_with_synonyms': terms_with_syn,
            'avg_text_length': sum(text_lengths) / len(text_lengths) if text_lengths else 0,
            'min_text_length': min(text_lengths) if text_lengths else 0,
            'max_text_length': max(text_lengths) if text_lengths else 0
        }


def main():
    parser = argparse.ArgumentParser(
        description="Process Radiology Terminology from Zip Archives",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Process LOINC
  python process_terminology.py --process loinc
  
  # Process RadLex
  python process_terminology.py --process radlex
  
  # Process both
  python process_terminology.py --process-all
        """
    )
    
    parser.add_argument('--process', choices=['loinc', 'radlex'],
                        help='Process specific terminology')
    parser.add_argument('--process-all', action='store_true',
                        help='Process all terminologies')
    
    args = parser.parse_args()
    
    if args.process == 'loinc':
        processor = LOINCProcessor()
        processor.process()
    elif args.process == 'radlex':
        processor = RadLexProcessor()
        processor.process()
    elif args.process_all:
        loinc_processor = LOINCProcessor()
        loinc_processor.process()
        
        print("\n")
        
        radlex_processor = RadLexProcessor()
        radlex_processor.process()
    else:
        parser.print_help()


if __name__ == '__main__':
    main()

