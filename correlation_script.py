# Build a correlation script that emphasizes country (jurisdiction), sector, and activities.
# Uses Amazon Bedrock AI for intelligent semantic matching
import pandas as pd
import re
import boto3
import json
from pathlib import Path
from datetime import datetime

COMP_PATH = Path("/home/sagemaker-user/shared/outputs/sec_matrix.csv")
REGS_PATH = Path("/home/sagemaker-user/shared/regulations_example.csv")
OUT_PATH  = Path("/home/sagemaker-user/shared/outputs/sec_x_laws_matches_strong.csv")

# Initialize Bedrock client
bedrock = boto3.client('bedrock-runtime', region_name='us-east-1')

def ai_similarity_score(text1: str, text2: str, field_type: str) -> float:
    """Use Bedrock AI to compute semantic similarity between two texts"""
    if not text1 or not text2:
        return 0.0
    
    prompt = f"""Compare these two {field_type} values and return a similarity score from 0.0 to 1.0:
Value 1: "{text1}"
Value 2: "{text2}"

Consider semantic equivalence (e.g., "US" = "United States", "tech" = "technology", "healthcare" = "health care") 
and meaning equivalence 
Return only the numeric score."""

    try:
        response = bedrock.invoke_model(
            modelId='anthropic.claude-3-haiku-20240307-v1:0',
            body=json.dumps({
                "anthropic_version": "bedrock-2023-05-31",
                "max_tokens": 50,
                "messages": [{"role": "user", "content": prompt}],
                "temperature": 0.0
            })
        )
        
        result = json.loads(response['body'].read())
        score_text = result['content'][0]['text'].strip()
        return float(re.findall(r'[0-1]\.?\d*', score_text)[0])
    except:
        return jaccard(text1, text2)  # Fallback to Jaccard

def normalize_sector(x: str) -> str:
    if not isinstance(x, str): 
        return ""
    xl = x.strip().lower()
    aliases = {
        "tech": "information technology",
        "it": "information technology",
        "communications": "communication services",
        "telecom": "communication services",
        "hc": "health care",
        "fin": "financials",
        "finance": "financials",
        "ind": "industrials",
        "mat": "materials",
        "staples": "consumer staples",
        "discretionary": "consumer discretionary",
        "reits": "real estate",
    }
    if xl in aliases:
        xl = aliases[xl]
    for s in {"communication services", "consumer discretionary", "consumer staples", "energy", "financials", "health care", "industrials", "information technology", "materials", "real estate", "utilities"}:
        if xl == s or s in xl or xl in s:
            return s
    return xl

def tokenize(s: str):
    if not isinstance(s, str) or not s.strip():
        return set()
    return set(re.findall(r"[a-z0-9\-\+_/]+", s.lower()))

def jaccard(a: str, b: str) -> float:
    ta, tb = tokenize(a), tokenize(b)
    if not ta and not tb:
        return 0.0
    inter = len(ta & tb)
    uni = len(ta | tb)
    return inter / uni if uni else 0.0

def date_overlap(sa, ea, sb, eb) -> float:
    def parse(x):
        try:
            return datetime.fromisoformat(x) if x and isinstance(x, str) else None
        except Exception:
            return None
    sa, ea, sb, eb = parse(sa), parse(ea), parse(sb), parse(eb)
    if sa is None and ea is None and sb is None and eb is None:
        return 0.0
    from datetime import datetime as dt
    ea = ea or dt.max
    eb = eb or dt.max
    sa = sa or dt.min
    sb = sb or dt.min
    latest_start = max(sa, sb)
    earliest_end = min(ea, eb)
    overlap = (earliest_end - latest_start).days
    if overlap <= 0:
        return 0.0
    union = (max(ea, eb) - min(sa, sb)).days
    return max(0.0, min(1.0, overlap / union)) if union > 0 else 0.0

# Load data
companies = pd.read_csv(COMP_PATH)

if REGS_PATH.exists():
    regulations = pd.read_csv(REGS_PATH)
else:
    regulations = pd.DataFrame(columns=["law_id","law_title","date","jurisdiction_country","sector","activities","regulatory_domain","impact_type","regulator_entity"])

comp_cols = {c.lower(): c for c in companies.columns}
def get_company_field(row, logical):
    mapping = {
        "ticker": ["ticker"],
        "company_name": ["company","company_name","name"],
        "jurisdiction_country": ["jurisdiction_country","country","headquarters_country","jurisdiction"],
        "sector": ["sector","gics_sector","industry","industry_sector"],
        "activities": ["activity","activities","business_function","segment_activity"],
        "regulatory_domain": ["regulatory_theme","theme","compliance_area"],
        "impact_type": ["impact_type","impact","risk_type"],
        "regulator_entity": ["regulator","regulator_entity"],
        "effective_start": ["effective_date_start","effective_start","fiscal_year_start","reporting_start"],
        "effective_end": ["effective_date_end","effective_end","fiscal_year_end","reporting_end"],
    }
    for cand in mapping.get(logical, []):
        if cand in comp_cols:
            val = row[comp_cols[cand]]
            if pd.notna(val):
                return val
    return ""

WEIGHTS = {
    "jurisdiction_country": 2.0,
    "sector": 1.8,
    "activities": 1.8,
    "regulatory_domain": 1.0,
    "impact_type": 0.8
}

rows = []
for i, crow in companies.iterrows():
    c_name = get_company_field(crow, "company_name")
    c_ticker = get_company_field(crow, "ticker")
    c_country = str(get_company_field(crow, "jurisdiction_country"))
    c_sector = normalize_sector(str(get_company_field(crow, "sector")))
    c_acts = str(get_company_field(crow, "activities"))
    c_theme = str(get_company_field(crow, "regulatory_domain"))
    c_impact = str(get_company_field(crow, "impact_type"))
    c_reg = str(get_company_field(crow, "regulator_entity"))
    c_s = get_company_field(crow, "effective_start")
    c_e = get_company_field(crow, "effective_end")

    for j, rrow in regulations.iterrows():
        # AI-powered semantic matching for key fields
        m_country = ai_similarity_score(c_country, str(rrow.get("jurisdiction_country","")), "country")
        m_sector = ai_similarity_score(c_sector, normalize_sector(str(rrow.get("sector",""))), "sector")
        m_acts = ai_similarity_score(c_acts, str(rrow.get("activities","")), "activities")
        m_theme = ai_similarity_score(c_theme, str(rrow.get("regulatory_domain","")), "regulatory_domain")
        
        # Standard matching for other fields
        m_impact = jaccard(c_impact, str(rrow.get("impact_type","")))
        m_reg = jaccard(c_reg, str(rrow.get("regulator_entity","")))
        m_date = date_overlap(c_s, c_e, str(rrow.get("date","")), None)

        score = (
            WEIGHTS["jurisdiction_country"] * m_country +
            WEIGHTS["sector"] * m_sector +
            WEIGHTS["activities"] * m_acts +
            WEIGHTS["regulatory_domain"] * m_theme +
            WEIGHTS["impact_type"] * m_impact
        )

        rows.append({
            "company_index": i,
            "law_index": j,
            "company_ticker": c_ticker,
            "company_name": c_name,
            "law_id": rrow.get("law_id",""),
            "law_title": rrow.get("law_title",""),
            "country_match": round(m_country, 3),
            "sector_match": round(m_sector, 3),
            "activities_match": round(m_acts, 3),
            "domain_match": round(m_theme, 3),
            "impact_match": round(m_impact, 3),
            "regulator_match": round(m_reg, 3),
            "date_overlap": round(m_date, 3),
            "score_total": round(score, 4),
        })

matches = pd.DataFrame(rows)
if not matches.empty:
    matches = matches.sort_values(["company_index","score_total"], ascending=[True, False]).reset_index(drop=True)
    matches["rank_for_company"] = matches.groupby("company_index")["score_total"].rank(method="first", ascending=False)
    matches.to_csv(OUT_PATH, index=False)
    print(f"AI-powered correlations saved to {OUT_PATH}")
    print(matches.head(50))

print(str(OUT_PATH))
