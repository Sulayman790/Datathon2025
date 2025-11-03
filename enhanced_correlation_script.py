import pandas as pd
import boto3
import json
import numpy as np
from pathlib import Path
from sklearn.metrics.pairwise import cosine_similarity

# AWS clients
bedrock = boto3.client('bedrock-runtime', region_name='us-west-2')
comprehend = boto3.client('comprehend', region_name='us-west-2')

COMP_PATH = Path("/home/sagemaker-user/shared/outputs/sec_matrix.csv")
REGS_PATH = Path("/home/sagemaker-user/shared/regulations_example.csv")
OUT_PATH = Path("/home/sagemaker-user/shared/outputs/enhanced_correlations.csv")

def get_embedding(text: str) -> np.ndarray:
    """Get text embedding using Bedrock Titan"""
    try:
        response = bedrock.invoke_model(
            modelId='amazon.titan-embed-text-v1',
            body=json.dumps({"inputText": str(text)[:8000]})
        )
        result = json.loads(response['body'].read())
        return np.array(result['embedding'])
    except:
        return np.zeros(1536)

def extract_entities(text: str) -> list:
    """Extract entities using Comprehend"""
    try:
        response = comprehend.detect_entities(
            Text=str(text)[:5000],
            LanguageCode='en'
        )
        return [entity['Text'].lower() for entity in response['Entities'] 
                if entity['Score'] > 0.8]
    except:
        return []

def semantic_similarity(text1: str, text2: str) -> float:
    """Calculate semantic similarity using embeddings"""
    emb1 = get_embedding(text1)
    emb2 = get_embedding(text2)
    return cosine_similarity([emb1], [emb2])[0][0]

def entity_overlap(text1: str, text2: str) -> float:
    """Calculate entity overlap using Comprehend"""
    entities1 = set(extract_entities(text1))
    entities2 = set(extract_entities(text2))
    if not entities1 and not entities2:
        return 0.0
    intersection = len(entities1 & entities2)
    union = len(entities1 | entities2)
    return intersection / union if union > 0 else 0.0

def enhanced_similarity(text1: str, text2: str, field_type: str) -> float:
    """Combined similarity using embeddings + entities"""
    semantic_score = semantic_similarity(text1, text2)
    entity_score = entity_overlap(text1, text2)
    
    # Weight based on field type
    weights = {
        'country': (0.8, 0.2),  # High semantic, low entity
        'sector': (0.6, 0.4),   # Balanced
        'activities': (0.5, 0.5), # Balanced
        'regulatory_domain': (0.7, 0.3)
    }
    
    w_semantic, w_entity = weights.get(field_type, (0.6, 0.4))
    return w_semantic * semantic_score + w_entity * entity_score

def run_enhanced_correlation():
    companies = pd.read_csv(COMP_PATH)
    regulations = pd.read_csv(REGS_PATH)
    
    comp_cols = {c.lower(): c for c in companies.columns}
    
    def get_field(row, logical):
        mapping = {
            "ticker": ["ticker"],
            "company_name": ["company", "company_name"],
            "jurisdiction_country": ["headquarters_country", "country"],
            "sector": ["sector"],
            "activities": ["activities", "business_function"],
            "regulatory_domain": ["regulatory_dependencies"]
        }
        for cand in mapping.get(logical, []):
            if cand in comp_cols and pd.notna(row[comp_cols[cand]]):
                return str(row[comp_cols[cand]])
        return ""
    
    WEIGHTS = {
        "jurisdiction_country": 2.0,
        "sector": 1.8,
        "activities": 1.8,
        "regulatory_domain": 1.0
    }
    
    rows = []
    total_pairs = len(companies) * len(regulations)
    
    for i, crow in companies.iterrows():
        c_ticker = get_field(crow, "ticker")
        c_name = get_field(crow, "company_name")
        c_country = get_field(crow, "jurisdiction_country")
        c_sector = get_field(crow, "sector")
        c_acts = get_field(crow, "activities")
        c_theme = get_field(crow, "regulatory_domain")
        
        for j, rrow in regulations.iterrows():
            print(f"Processing {i*len(regulations)+j+1}/{total_pairs}: {c_ticker}")
            
            # Enhanced semantic matching
            m_country = enhanced_similarity(c_country, str(rrow.get("jurisdiction_country", "")), "country")
            m_sector = enhanced_similarity(c_sector, str(rrow.get("sector", "")), "sector")
            m_acts = enhanced_similarity(c_acts, str(rrow.get("activity", "")), "activities")
            m_theme = enhanced_similarity(c_theme, str(rrow.get("regulatory_domain", "")), "regulatory_domain")
            
            score = (
                WEIGHTS["jurisdiction_country"] * m_country +
                WEIGHTS["sector"] * m_sector +
                WEIGHTS["activities"] * m_acts +
                WEIGHTS["regulatory_domain"] * m_theme
            )
            
            rows.append({
                "company_ticker": c_ticker,
                "company_name": c_name,
                "law_id": rrow.get("law_id", ""),
                "country_match": round(m_country, 3),
                "sector_match": round(m_sector, 3),
                "activities_match": round(m_acts, 3),
                "domain_match": round(m_theme, 3),
                "score_total": round(score, 4)
            })
    
    matches = pd.DataFrame(rows)
    matches = matches.sort_values(["company_ticker", "score_total"], ascending=[True, False])
    matches.to_csv(OUT_PATH, index=False)
    
    print(f"\nEnhanced correlations saved to: {OUT_PATH}")
    print(f"Top 10 matches:")
    print(matches.head(10))
    
    return matches

if __name__ == "__main__":
    matches = run_enhanced_correlation()