"""
RadLex Processing Strategy:
- One term = one chunk
- Include: ID, preferred term, definition, synonyms, parent concepts
- Preserve hierarchical relationships in metadata
"""

import xml.etree.ElementTree as ET
from pathlib import Path
from typing import List, Dict
import json
from datetime import datetime

import sys
sys.path.append(str(Path(__file__).parent.parent))

from models.chunk import Chunk


class RadLexProcessor:
    """Process RadLex ontology into structured chunks"""
    
    def __init__(self, radlex_dir: Path):
        self.radlex_dir = radlex_dir
        self.chunks: List[Chunk] = []
        self.owl_file = radlex_dir / "RadLex.owl"
    
    def load_radlex(self) -> List[Dict]:
        """
        Load RadLex data from OWL file.
        RadLex is in OWL/RDF format with terms, definitions, and hierarchies.
        """
        print(f"   Loading RadLex from: {self.owl_file}")
        
        if not self.owl_file.exists():
            raise FileNotFoundError(f"RadLex OWL file not found: {self.owl_file}")
        
        with open(self.owl_file, 'rb') as f:
            owl_content = f.read()
        
        terms = self._parse_owl(owl_content)
        print(f"   Parsed {len(terms):,} RadLex terms")
        
        return terms
    
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
            
            # Define namespaces
            namespaces = {
                'owl': 'http://www.w3.org/2002/07/owl#',
                'rdf': 'http://www.w3.org/1999/02/22-rdf-syntax-ns#',
                'rdfs': 'http://www.w3.org/2000/01/rdf-schema#',
                'skos': 'http://www.w3.org/2004/02/skos/core#',
                'obo': 'http://purl.obolibrary.org/obo/',
                'dc': 'http://purl.org/dc/elements/1.1/',
                'oboInOwl': 'http://www.geneontology.org/formats/oboInOwl#',
                'RID': 'http://www.radlex.org/RID/'
            }
            
            # Find all classes (terms)
            owl_classes = (
                root.findall('.//owl:Class', namespaces) or 
                root.findall('.//{http://www.w3.org/2002/07/owl#}Class')
            )
            
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
                
                # Get label - prioritize RadLex Preferred_name
                label = ''
                
                # Try RadLex-specific Preferred_name
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
                
                # Fallback to prefLabel
                if not label:
                    pref_label = owl_class.find('skos:prefLabel', namespaces)
                    if pref_label is not None and pref_label.text:
                        label = pref_label.text.strip()
                
                # Get definition
                definition = ''
                
                # Try RadLex-specific Definition
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
                
                # Get synonyms
                synonyms = []
                
                # Try RadLex-specific Synonym
                for syn_elem in owl_class.findall('RID:Synonym', namespaces):
                    if syn_elem.text:
                        syn_text = syn_elem.text.strip()
                        if syn_text and syn_text not in synonyms:
                            synonyms.append(syn_text)
                
                # Also try Acronym as synonym
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
                        'label': label if label else rid,
                        'definition': definition,
                        'synonyms': synonyms,
                        'parents': parents,
                        'uri': term_id
                    }
                    terms.append(term)
        
        except ET.ParseError as e:
            print(f"   ⚠️  Warning: XML parse error: {e}")
            raise
        
        return terms
    
    def process(self) -> List[Chunk]:
        """
        Process RadLex into chunks.
        
        Each chunk contains:
        - RadLex ID
        - Preferred term
        - Definition
        - All synonyms
        - Parent concepts
        - Related terms if available
        """
        print("\n   Processing RadLex terms into chunks...")
        
        # Load terms
        terms = self.load_radlex()
        
        # Create chunks
        for term in terms:
            # Build rich text representation
            text_parts = []
            
            # Main term
            text_parts.append(f"RadLex ID: {term['rid']}")
            text_parts.append(f"Term: {term['label']}")
            
            # Definition
            if term.get('definition'):
                text_parts.append(f"Definition: {term['definition']}")
            
            # Synonyms
            if term.get('synonyms'):
                text_parts.append(f"Synonyms: {', '.join(term['synonyms'])}")
            
            # Parent terms (hierarchy)
            if term.get('parents'):
                text_parts.append(f"Parent terms: {', '.join(term['parents'])}")
            
            chunk_text = '\n'.join(text_parts)
            
            # Create chunk with metadata
            chunk = Chunk(
                chunk_id=f"radlex_{term['rid']}",
                text=chunk_text,
                source_type="radlex",
                metadata={
                    "source_file": "RadLex.owl",
                    "created_at": datetime.now().isoformat(),
                    "term_id": term['rid'],
                    "label": term['label'],
                    "has_definition": bool(term.get('definition')),
                    "synonym_count": len(term.get('synonyms', [])),
                    "parent_count": len(term.get('parents', [])),
                    "category": "terminology",
                    "char_count": len(chunk_text)
                }
            )
            
            self.chunks.append(chunk)
        
        print(f"   ✓ Created {len(self.chunks):,} chunks")
        
        return self.chunks
    
    def save_chunks(self, output_path: Path):
        """Save chunks to JSONL"""
        print(f"   Saving to: {output_path}")
        
        with open(output_path, 'w', encoding='utf-8') as f:
            for chunk in self.chunks:
                f.write(json.dumps(chunk.to_dict()) + '\n')
        
        print(f"   ✓ Saved {len(self.chunks):,} chunks")
