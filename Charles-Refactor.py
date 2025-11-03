#!/usr/bin/env python
# coding: utf-8

get_ipython().system('pip install langdetect spacy beautifulsoup4 lxml instructor pydantic')
get_ipython().system('python -m spacy download en_core_web_sm')


# # Setup, configuration, file & HTML utilities

# %% [markdown]
# ## 1) Setup, configuration, file & HTML utilities
# - Imports and environment configuration (AWS region, model IDs, paths, size limits)
# - AWS clients (Translate + Bedrock)
# - Stopwords, regexes, language constants
# - File discovery & deduplication helpers
# - HTML parsing and main-text extraction helpers

# %%
import os, re, json, html, string, unicodedata
from pathlib import Path
from datetime import datetime, timezone, date
import boto3
import pandas as pd
from bs4 import BeautifulSoup
from dateutil.parser import parse as parse_date
from langdetect import detect, detect_langs, DetectorFactory
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.feature_extraction import text as sk_text

DetectorFactory.seed = 42

AWS_REGION=os.getenv("AWS_REGION","us-west-2")
MODEL_TRANSLATE_FALLBACK_1=os.getenv("BEDROCK_TRANSLATE_PRIMARY","amazon.nova-micro-v1:0")
MODEL_TRANSLATE_FALLBACK_2=os.getenv("BEDROCK_TRANSLATE_SECONDARY","anthropic.claude-3-7-sonnet-20250219-v1:0")
MODEL_EXTRACT=os.getenv("BEDROCK_EXTRACT_MODEL","amazon.nova-premier-v1:0")
MODEL_EXTRACT_DOC=os.getenv("BEDROCK_EXTRACT_DOC_MODEL","amazon.nova-premier-v1:0:1000k")

INPUT_DIRS=json.loads(os.getenv("INPUT_DIRS","[\"./directives\"]"))
ALLOWED_EXT=set([x.lower().strip() for x in json.loads(os.getenv("ALLOWED_EXT","[\".html\",\".xml\"]"))])
RECURSIVE=os.getenv("RECURSIVE","true").strip().lower() in {"1","true","yes"}
OUT_DIR=os.getenv("OUT_DIR","out")
MAX_DOC_CHARS=int(os.getenv("MAX_DOC_CHARS","180000"))
MAX_CHUNK_CHARS=int(os.getenv("MAX_CHUNK_CHARS","18000"))

translate=boto3.client("translate",region_name=AWS_REGION)
bedrock=boto3.client("bedrock-runtime",region_name=AWS_REGION)

EXTRA_STOPWORDS={"section","sections","article","articles","annex","annexe","appendix","appendice","subtitle","title","chapter","chapitre","directive","regulation","regulations","law","act","union","paragraph","subparagraph","recital","dispositif","premier","1er","amended","shall","must","may","including","include","pursuant","accordance","specified","provide","provided","applicable","applicability","applying","applicant","applicants","applying","subject","subjects","thereof","hereof","therein","herein","thereby","hereby","whereas","hereunder","thereunder","among","between","within","without","preamble","scope","purpose","purposes","general","specific"}
STOPWORDS_EN=set(sk_text.ENGLISH_STOP_WORDS)|EXTRA_STOPWORDS

STOPWORDS_JA={"第","条","項","号","章","節","款","目次","附則","総則","抄","同","前","又は","及び","並びに","その他","こと","もの","ため","者","うえ","上","下","について","に関する","に係る","する","される","した","して","すると","され","なる","ない","これ","それ","当該","各","同条","政府","国","内閣","大臣","本部","本部長","本法","本章","本条","次項","前項","人工知能","人工知能関連技術","技術","研究開発","活用","推進","計画","基本","基本計画","施策","規定","規範","方針","必要","措置","整備","確保","促進","国際","協力","教育","人材","情報","データ","等","など"}
STRUCTURAL_LABEL_JA={"総則","附則","目次","人工知能基本計画","人工知能戦略本部"}

PARLIAMENTARY=re.compile(r"^\s*(having regard|after transmission|after consulting|in accordance with|whereas|pursuant to|considering|vu(?:\s+la|(?:x|es)?)?)\b.*$",re.IGNORECASE|re.MULTILINE)
RECITALS=re.compile(r"\b(whereas|considérant(?:\s+que)?|vu(?:\s+la|(?:x|es)?)?)\b.*?(?=(^|\n)\s*(article|art\.?|dispositif|chapitre|titre)\s*[^\n]*\b(1|premier|1er)\b|\bannex|annexe|appendix|appendice|schedule\b)",re.IGNORECASE|re.DOTALL)
TAIL=re.compile(r"(done at\s+[A-Za-z]+\s+\d{1,2}\s+[A-Za-z]+\s+\d{4}.*$|for the european parliament.*$|for the council.*$)",re.IGNORECASE|re.DOTALL)
DATE_RX=re.compile(r"(\b\d{1,2}\s+(jan|feb|mar|apr|may|jun|jul|aug|sep|sept|oct|nov|dec)[a-z]*\s+\d{4}\b|\b\d{4}-\d{1,2}-\d{1,2}\b|\b\d{1,2}[./]\d{1,2}[./]\d{2,4}\b)",re.IGNORECASE)
CJK_DATE_RX=re.compile(r"(\d{4})年(\d{1,2})月(\d{1,2})日")
JP_ERA=[("令和",2019),("平成",1989),("昭和",1926),("大正",1912),("明治",1868)]

def _resolve_input_dir():
    for d in INPUT_DIRS:
        p=Path(d)
        if p.exists(): return p
    p=Path("./directives"); p.mkdir(parents=True,exist_ok=True); return p

def _canon_name(p: Path):
    stem=p.stem
    stem=re.sub(r"(?i)(^|[-_\s])(checkpoint|copy|copie|copiar)$","",stem).strip()
    stem=re.sub(r"(?i)[-_]checkpoint","",stem)
    stem=re.sub(r"\s*\(\d+\)$","",stem)
    stem=re.sub(r"\s+"," ",stem)
    return (stem.lower(), p.suffix.lower())

def _is_checkpoint(p: Path):
    name=p.name.lower()
    return ("-checkpoint" in name or name.endswith("_checkpoint.html") or "checkpoint.html" in name or name.endswith(".ipynb") or p.parent.name.lower()==".ipynb_checkpoints" or ".ipynb_checkpoints" in str(p.parent).lower())

def _list_files(root,recursive=False):
    def ok(p):
        n=p.name
        if _is_checkpoint(p): return False
        if p.parent.name == ".ipynb_checkpoints": return False
        return p.is_file() and p.suffix.lower() in ALLOWED_EXT and not (n.startswith(".") or n.startswith("._") or n.endswith("~"))
    it=(root.rglob("*") if recursive else root.iterdir())
    cand=[p for p in it if ok(p)]
    grouped={}
    for p in cand:
        key=_canon_name(p); best=grouped.get(key)
        if best is None: grouped[key]=p
        else:
            if _is_checkpoint(best) and not _is_checkpoint(p): grouped[key]=p
            elif _is_checkpoint(best)==_is_checkpoint(p):
                if p.stat().st_mtime>best.stat().st_mtime: grouped[key]=p
    files=sorted(grouped.values(),key=lambda x:x.name.lower())
    print(f"[INPUT] {len(files)} files in {root} (recursive={recursive})")
    for p in files: print(" -",p.name)
    return files

def _read(path): return Path(path).read_text(encoding="utf-8",errors="ignore")

def _extract_main_container(soup):
    for sel in ["#innerDocument","main#contentsLaw","#docHtml","article","#content","body"]:
        el=soup.select_one(sel)
        if el and len(el.get_text(strip=True))>200: return el
    return soup

def _html_to_text(raw):
    try: soup=BeautifulSoup(raw,"lxml")
    except Exception: soup=BeautifulSoup(raw,"html.parser")
    main=_extract_main_container(soup)
    for tag in main(["script","style","nav","header","footer","noscript","aside","form"]):
        try: tag.extract()
        except Exception: pass
    for br in main.find_all(["br","hr"]): br.replace_with("\n")
    for li in main.find_all("li"):
        txt=li.get_text(" ",strip=True); li.string=("\n- "+txt+"\n") if txt else "\n"
    for th in main.find_all(["h1","h2","h3","h4","h5","h6","strong","b"]):
        t=th.get_text(" ",strip=True); th.string=("\n"+t+"\n") if t else "\n"
    text=html.unescape(main.get_text("\n",strip=True))
    text=re.sub(r"\n{3,}","\n\n",text)
    return text,soup

def _extract_main_title(soup, fallback):
    try: t=soup.title.get_text(strip=True) if soup and soup.title else ""
    except Exception: t=""
    try: h=soup.find(["h1","h2"]); h1=h.get_text(" ",strip=True) if h else ""
    except Exception: h1=""
    return next((x for x in [h1,t,fallback] if x),fallback)


# # Language detection/translation, date parsing, NLP extraction & aggregation
# 

# %% [markdown]
# ## 2) Language detection/translation, date parsing, NLP extraction & aggregation
# - Detects English and translates (Amazon Translate → Bedrock fallbacks), chunking long texts
# - Prunes recitals/tails/TOC to keep operative content
# - Parses dates (regex, CJK, Japanese eras)
# - Extracts per-chunk and per-document fields via Bedrock LLMs
# - Builds TF-IDF keywords and normalizes/aggregates all fields into one row

# %%
def _has_cjk(s): return bool(re.search(r"[\u3400-\u4dbf\u4e00-\u9fff\u3040-\u30ff]",s or ""))

def _lang_probs(text):
    try: return detect_langs(text)
    except Exception:
        try: return [type("LP",(object,),{"lang":detect(text),"prob":1.0})()]
        except Exception: return []

def _english_confidence(text):
    t=(text or "").strip()
    if not t: return 1.0
    sample=t[:8000]
    letters=[ch for ch in sample if ch.isalpha()]
    ascii_letters=[ch for ch in letters if ("A"<=ch<="Z") or ("a"<=ch<="z")]
    ascii_ratio=(len(ascii_letters)/max(1,len(letters))) if letters else 0.0
    tokens=[w.strip(string.punctuation).lower() for w in re.split(r"\s+",sample) if w]
    stop_hits=sum(1 for tok in tokens if tok in sk_text.ENGLISH_STOP_WORDS)
    stop_ratio=stop_hits/max(1,len(tokens))
    ld_prob=0.0
    for lp in _lang_probs(sample):
        if getattr(lp,"lang","")=="en": ld_prob=max(ld_prob,float(getattr(lp,"prob",0.0)))
    score=0.75*ld_prob+0.25*(0.6*ascii_ratio+0.4*stop_ratio)
    return max(0.0,min(1.0,score))

def _is_english(text):
    if not text or len(text.strip())==0: return True
    if _has_cjk(text):
        try: return detect(text)=="en"
        except Exception: return False
    if len(text)<160:
        try: return detect(text)=="en"
        except Exception: return False
    return _english_confidence(text)>=0.55

def _translate_piece_bedrock(piece,model_id,system_prompt=None):
    body={"max_tokens":4000,"temperature":0.0}
    if "anthropic" in model_id:
        body={"anthropic_version":"bedrock-2023-05-31","max_tokens":4000,"temperature":0.0,"messages":[{"role":"user","content":[{"type":"text","text":"Translate this to precise legal English. Keep headings, numbering, dates, entities verbatim. No commentary.\n\n"+piece}]}]}
    else:
        prompt="Translate the following text into precise legal English. Preserve headings, numbering, and dates. No commentary.\n\n"+piece
        body={"inputText":prompt,"textGenerationConfig":{"maxTokenCount":4000,"temperature":0.0}}
    try:
        resp=bedrock.invoke_model(modelId=model_id,body=json.dumps(body))
        data=json.loads(resp["body"].read())
        if "anthropic" in model_id: return data.get("content",[{}])[0].get("text","")
        return data.get("outputText","")
    except Exception:
        return ""

def _chunk_for_translate(t,limit=4200):
    out=[]; i=0; n=len(t)
    seps=["\n\n","。\n","。\n\n","；","；\n","；\n\n","，","。\n—\n","\n- "]
    while i<n:
        j=min(i+limit,n)
        k=-1
        for sep in seps:
            ks=t.rfind(sep,i,j)
            if ks>k: k=ks+len(sep)
        if k<i+200: k=j
        piece=t[i:k].strip(); i=k
        if piece: out.append(piece)
    return out

def _force_english(text):
    if not text: return text
    if _is_english(text): return text
    chunks=_chunk_for_translate(text,limit=4000)
    out=[]
    for piece in chunks:
        ok=False
        try:
            r=translate.translate_text(Text=piece,SourceLanguageCode="auto",TargetLanguageCode="en")
            cand=r.get("TranslatedText","") or ""
            if cand.strip() and _is_english(cand): out.append(cand); ok=True
        except Exception: pass
        if not ok:
            cand=_translate_piece_bedrock(piece,MODEL_TRANSLATE_FALLBACK_1)
            if cand.strip() and _is_english(cand): out.append(cand); ok=True
        if not ok:
            cand=_translate_piece_bedrock(piece,MODEL_TRANSLATE_FALLBACK_2)
            if cand.strip():
                out.append(cand if _is_english(cand) else _translate_piece_bedrock(cand,MODEL_TRANSLATE_FALLBACK_1))
    final="\n".join([c for c in out if c]).strip()
    return final if final else text

def _translate(text):
    t=(text or "").strip()
    if not t: return t
    if _is_english(t): return t
    out=_force_english(t)
    if not _is_english(out): out=_force_english(out)
    return out

def _prune_operative(text_en):
    x=PARLIAMENTARY.sub("",text_en)
    x=RECITALS.sub("",x)
    x=TAIL.sub("",x)
    x=re.sub(r"\bTable of Contents\b.*?(?=(^|\n)\s*Article\s+(1|premier|1er)\b)", "", x, flags=re.IGNORECASE|re.DOTALL)
    return x.strip()

def _chunk_iter(text,limit=MAX_CHUNK_CHARS):
    i=0; n=len(text)
    seps=["\n\n","\n- ","; ",". "]
    while i<n:
        j=min(i+limit,n)
        k=-1
        for sep in seps:
            ks=text.rfind(sep,i,j)
            if ks>k: k=ks+len(sep)
        if k<i+200: k=j
        piece=text[i:k].strip(); i=k
        if piece: yield piece

def _to_ymd(s):
    try: return parse_date(s,fuzzy=True,dayfirst=False).date().isoformat()
    except Exception: return ""

def _parse_cjk_date(s):
    m=CJK_DATE_RX.search(s)
    if not m: return ""
    y=int(m.group(1)); mth=int(m.group(2)); d=int(m.group(3))
    try: return date(y,mth,d).isoformat()
    except Exception: return ""

def _parse_jp_era(s):
    m=re.search(r"(令和|平成|昭和|大正|明治)\s*([元\d]+)\s*年\s*([0-9]{1,2})\s*月\s*([0-9]{1,2})\s*日",s)
    if not m: return ""
    era=m.group(1); year=m.group(2); month=int(m.group(3)); day=int(m.group(4))
    base=dict(JP_ERA)[era]; y=1 if year=="元" else int(year)
    try: return date(base+y-1,month,day).isoformat()
    except Exception: return ""

def _doc_date(raw_text,soup,fname):
    cands=[]
    try: cands.extend(el.get_text(" ",strip=True) for el in soup.select("#lawTitleNo,#lawTitle,.oj-hd-date,.oj-doc-ti,.date,.document-date,.pubdate,.issued,.enacted,.approved,#lawTitleNo"))
    except Exception: pass
    head=" ".join(cands)
    jp=_parse_jp_era(head) or _parse_jp_era(raw_text[:120000])
    if jp: return jp
    cjk=_parse_cjk_date(head) or _parse_cjk_date(raw_text[:120000])
    if cjk: return cjk
    for m in DATE_RX.findall(head):
        d=_to_ymd(m[0])
        if d: return d
    for m in DATE_RX.findall(raw_text[:120000]):
        d=_to_ymd(m[0])
        if d: return d
    m=re.search(r"(\d{4})[-_](\d{1,2})[-_](\d{1,2})",Path(fname).name)
    if m:
        try: return datetime(int(m.group(1)),int(m.group(2)),int(m.group(3))).date().isoformat()
        except Exception: pass
    m=re.search(r"\b(19|20)\d{2}\b",Path(fname).name)
    if m:
        try: return datetime(int(m.group(0)),6,30).date().isoformat()
        except Exception: pass
    try:
        ts=Path(fname).stat().st_mtime
        return datetime.fromtimestamp(ts,tz=timezone.utc).date().isoformat()
    except Exception:
        return datetime.now(timezone.utc).date().isoformat()

def _ensure_ascii_lower(s: str):
    s=(s or "").strip()
    s=unicodedata.normalize("NFKD", s).encode("ascii","ignore").decode("ascii")
    s=re.sub(r"[^A-Za-z0-9%/_\-\s]", "", s)
    s=re.sub(r"\s+"," ", s).strip().lower()
    return s

def _kw_from_text(text_en, max_k=40):
    try:
        vec=TfidfVectorizer(stop_words=list(STOPWORDS_EN),ngram_range=(1,2),min_df=1,token_pattern=r"(?u)\b[a-zA-Z][a-zA-Z%/_-]+\b")
        X=vec.fit_transform([text_en]); scores=X.toarray()[0]; terms=vec.get_feature_names_out()
        pairs=sorted(zip(terms,scores),key=lambda x:-x[1])
        bad={"article","articles","annex","section","chapter","law","act","directive","regulation","regulations","union","paragraph","recital","subparagraph","subtitle","title"}
        out=[]
        for t,_ in pairs:
            tl=_ensure_ascii_lower(t)
            if tl in bad or tl in STOPWORDS_EN: continue
            if re.fullmatch(r"\d+(\.\d+)?",tl): continue
            if len(tl)<=2: continue
            if tl not in out: out.append(tl)
            if len(out)>=max_k: break
        return out
    except Exception:
        return []

def _llm_extract_chunk(chunk_en):
    prompt=("Return a strict JSON object with keys exactly:\n"
            "jurisdiction (string), sector (array of strings), activity (array of strings), regulatory_theme (array of strings), impact_type (array of strings), regulator (array of strings), company_country (array of strings), effective_date (YYYY-MM-DD or \"\").\n"
            "Use only this text, be concise, lowercase labels except jurisdictions/regulators/countries (title case), deduplicate.\n\nTEXT:\n"+chunk_en[:MAX_CHUNK_CHARS])
    body={"messages":[{"role":"user","content":[{"type":"text","text":prompt}]}],"max_tokens":3000,"temperature":0.0}
    try:
        resp=bedrock.invoke_model(modelId=MODEL_EXTRACT,body=json.dumps(body))
        parsed=json.loads(resp["body"].read())
        text_out=parsed.get("content",[{}])[0].get("text","{}")
        data=json.loads(text_out)
        def normlist(v):
            if isinstance(v,list): return [str(x).strip() for x in v if str(x).strip()]
            if isinstance(v,str) and v.strip(): return [v.strip()]
            return []
        return {
            "jurisdiction":str(data.get("jurisdiction","")).strip(),
            "sector":normlist(data.get("sector",[])),
            "activity":normlist(data.get("activity",[])),
            "regulatory_theme":normlist(data.get("regulatory_theme",[])),
            "impact_type":normlist(data.get("impact_type",[])),
            "regulator":normlist(data.get("regulator",[])),
            "company_country":normlist(data.get("company_country",[])),
            "effective_date":str(data.get("effective_date","")).strip()
        }
    except Exception:
        return {"jurisdiction":"","sector":[],"activity":[],"regulatory_theme":[],"impact_type":[],"regulator":[],"company_country":[],"effective_date":""}

def _llm_extract_doc(title, text_en):
    prompt=("From the following title and document, infer a single JSON object with keys exactly:\n"
            "jurisdiction (string), sector (array), activity (array), regulatory_theme (array), impact_type (array), regulator (array), company_country (array), default_effective_date (YYYY-MM-DD or \"\").\n\n"
            "TITLE:\n"+(title or "")+"\n\nDOCUMENT:\n"+(text_en[:MAX_DOC_CHARS] or ""))
    body={"messages":[{"role":"user","content":[{"type":"text","text":prompt}]}],"max_tokens":6000,"temperature":0.0}
    try:
        resp=bedrock.invoke_model(modelId=MODEL_EXTRACT_DOC,body=json.dumps(body))
        parsed=json.loads(resp["body"].read())
        text_out=parsed.get("content",[{}])[0].get("text","{}")
        data=json.loads(text_out)
        def normlist(v):
            if isinstance(v,list): return [str(x).strip() for x in v if str(x).strip()]
            if isinstance(v,str) and v.strip(): return [v.strip()]
            return []
        return {
            "jurisdiction":str(data.get("jurisdiction","")).strip(),
            "sector":normlist(data.get("sector",[])),
            "activity":normlist(data.get("activity",[])),
            "regulatory_theme":normlist(data.get("regulatory_theme",[])),
            "impact_type":normlist(data.get("impact_type",[])),
            "regulator":normlist(data.get("regulator",[])),
            "company_country":normlist(data.get("company_country",[])),
            "default_effective_date":str(data.get("default_effective_date","")).strip()
        }
    except Exception:
        return {"jurisdiction":"","sector":[],"activity":[],"regulatory_theme":[],"impact_type":[],"regulator":[],"company_country":[],"default_effective_date":""}

def _to_title(s):
    s=_ensure_ascii_lower(s)
    return " ".join([w.capitalize() for w in s.split()])

def _norm_titlecase(items): return sorted(list(dict.fromkeys([_to_title(x) for x in items if x.strip()])))
def _norm_lower(items): return sorted(list(dict.fromkeys([_ensure_ascii_lower(x) for x in items if _ensure_ascii_lower(x)])))

def _aggregate_fields(doc_backfill, chunk_fields_list, corpus_text_en, doc_date_guess):
    juris=doc_backfill.get("jurisdiction","").strip() or ""
    sectors=set(doc_backfill.get("sector",[]))
    activities=set(doc_backfill.get("activity",[]))
    themes=set(doc_backfill.get("regulatory_theme",[]))
    impacts=set(doc_backfill.get("impact_type",[]))
    regulators=set(doc_backfill.get("regulator",[]))
    countries=set(doc_backfill.get("company_country",[]))
    eff=doc_backfill.get("default_effective_date","").strip() or ""

    for cf in chunk_fields_list:
        if not juris and cf.get("jurisdiction",""): juris=cf["jurisdiction"]
        sectors.update(cf.get("sector",[]))
        activities.update(cf.get("activity",[]))
        themes.update(cf.get("regulatory_theme",[]))
        impacts.update(cf.get("impact_type",[]))
        regulators.update(cf.get("regulator",[]))
        countries.update(cf.get("company_country",[]))
        if not eff and cf.get("effective_date",""): eff=cf["effective_date"]

    if not eff:
        head=corpus_text_en[:8000]
        m=DATE_RX.search(head)
        if m: eff=_to_ymd(m[0]) or eff
        if not eff: eff=doc_date_guess

    juris = _to_title(juris) if juris else "Global"
    regulators=_norm_titlecase(regulators)
    countries=_norm_titlecase(countries) if countries else ([juris] if juris!="Global" else ["Global"])
    sectors=_norm_lower(sectors) or ["general"]
    activities=_norm_lower(activities) or ["general"]
    themes=_norm_lower(themes) or ["general"]
    impacts=_norm_lower(impacts) or ["obligation"]
    eff = eff or ""

    kws=_kw_from_text(corpus_text_en, max_k=50)

    return {
        "jurisdiction": juris,
        "sector": sectors,
        "activity": activities,
        "regulatory_theme": themes,
        "impact_type": impacts,
        "effective_date": eff,
        "regulator": regulators or ["General Regulator"],
        "keywords": kws,
        "company_country": countries
    }


# # Serialization, saving & pipeline orchestration

# %% [markdown]
# ## 3) Serialization, saving & pipeline orchestration
# - Serializes arrays as JSON strings for CSV columns
# - Saves one CSV per input and a combined "ALL" CSV
# - `process_file`: end-to-end for one document (parse → translate → prune → extract → aggregate)
# - `process_all_documents`: loops over files and returns a DataFrame
# - Main guard to run the batch when executed as a script

# %%
def _serialize_row(row):
    def J(x): return json.dumps(x, ensure_ascii=False)
    return {
        "jurisdiction": row["jurisdiction"],
        "sector": J(sorted(row["sector"])),
        "activity": J(sorted(row["activity"])),
        "regulatory_theme": J(sorted(row["regulatory_theme"])),
        "impact_type": J(sorted(row["impact_type"])),
        "effective_date": row["effective_date"],
        "regulator": J(sorted(row["regulator"])),
        "keywords": J(row["keywords"]),
        "company_country": J(sorted(row["company_country"]))
    }

def _save_per_input(row,input_path):
    Path(OUT_DIR).mkdir(parents=True,exist_ok=True)
    stem=input_path.stem
    csv_path=f"{OUT_DIR}/{stem}.csv"
    df=pd.DataFrame([_serialize_row(row)],columns=["jurisdiction","sector","activity","regulatory_theme","impact_type","effective_date","regulator","keywords","company_country"])
    df.to_csv(csv_path,index=False)
    print(f"[SAVE] {csv_path}")

def _save_all(rows):
    Path(OUT_DIR).mkdir(parents=True,exist_ok=True)
    csv_path=f"{OUT_DIR}/Regulatory_Extraction_ALL.csv"
    df=pd.DataFrame([_serialize_row(r) for r in rows],columns=["jurisdiction","sector","activity","regulatory_theme","impact_type","effective_date","regulator","keywords","company_country"])
    df.to_csv(csv_path,index=False)
    print(f"[SAVE] {csv_path}")

def process_file(path: Path):
    raw=_read(path)
    raw_txt,soup=_html_to_text(raw)
    title=_extract_main_title(soup, path.stem)
    en=_translate(raw_txt)
    if not _is_english(en): en=_force_english(en)
    operative=_prune_operative(en)
    doc_date_guess=_doc_date(raw_txt,soup,str(path))
    back=_llm_extract_doc(title, operative)

    chunk_fields=[]
    for chunk in _chunk_iter(operative, limit=MAX_CHUNK_CHARS):
        c_en=_translate(chunk)
        if not _is_english(c_en): c_en=_force_english(c_en)
        f=_llm_extract_chunk(c_en)
        chunk_fields.append(f)

    row=_aggregate_fields(back, chunk_fields, operative, back.get("default_effective_date") or doc_date_guess)
    return row

def process_all_documents():
    root=_resolve_input_dir()
    files=_list_files(root,recursive=RECURSIVE)
    if not files:
        print("[WARN] no inputs"); return pd.DataFrame()
    all_rows=[]
    for f in files:
        row=process_file(f)
        _save_per_input(row,f)
        all_rows.append(row)
        print(f"[FILE] {Path(f).name}: 1 row")
    _save_all(all_rows)
    df=pd.DataFrame([_serialize_row(r) for r in all_rows],columns=["jurisdiction","sector","activity","regulatory_theme","impact_type","effective_date","regulator","keywords","company_country"])
    print(f"[TOTAL] {len(df)} rows")
    return df

if __name__=="__main__":
    df=process_all_documents()
    print(df.head() if not df.empty else "No results")


# # Translation with English recognition

import json
from pathlib import Path
import boto3
from botocore.config import Config
from bs4 import BeautifulSoup
from langdetect import detect, DetectorFactory
from concurrent.futures import ThreadPoolExecutor

AWS_REGION = "us-west-2"
OUT_DIR = "out/translate"
MAX_CHUNK_CHARS = 4000

DetectorFactory.seed = 42

bedrock = boto3.client("bedrock-runtime", region_name=AWS_REGION, config=Config(read_timeout=60, retries={"max_attempts": 3}))

PROFILE_IDS = {
    "anthropic_haiku_4_5": "global.anthropic.claude-haiku-4-5-20251001-v1:0",
    "anthropic_sonnet_4_5": "global.anthropic.claude-sonnet-4-5-20250929-v1:0",
    "anthropic_sonnet_4": "global.anthropic.claude-sonnet-4-20250514-v1:0"
}

def log(msg):
    print(f"[LOG] {msg}")

def get_files():
    p = Path("./directives")
    if not p.exists():
        log("directives folder not found")
        return []
    files = [f for f in p.iterdir() if f.is_file() and f.suffix.lower() in {".html", ".xml"}]
    log(f"Detected {len(files)} eligible files in directives folder")
    return files

def html_to_text(raw):
    soup = BeautifulSoup(raw, "html.parser")
    for tag in soup(["script", "style", "nav", "header", "footer"]):
        tag.extract()
    text = soup.get_text(" ", strip=True)
    log(f"Extracted {len(text)} characters of text from HTML")
    return text

def chunk_text(text, limit=MAX_CHUNK_CHARS):
    chunks, i = [], 0
    while i < len(text):
        end = min(i + limit, len(text))
        if end < len(text):
            for sep in [". ", ".\n", "! ", "? "]:
                k = text.rfind(sep, i, end)
                if k > i + 200:
                    end = k + len(sep)
                    break
        chunks.append(text[i:end].strip())
        i = end
    log(f"Split text into {len(chunks)} chunks of up to {limit} characters")
    return [c for c in chunks if c]

def invoke_anthropic_profile(profile_id, user_text, max_tokens=4000, temperature=0.0):
    body = {
        "anthropic_version": "bedrock-2023-05-31",
        "max_tokens": max_tokens,
        "temperature": temperature,
        "messages": [{"role": "user", "content": [{"type": "text", "text": user_text}]}],
    }
    resp = bedrock.invoke_model(modelId=profile_id, body=json.dumps(body))
    data = json.loads(resp["body"].read())
    return (data.get("content", [{}])[0].get("text") or "").strip()

def llm_is_english(sample_text):
    log("Checking language of text with LLM fallback...")
    q = "Answer with exactly 'en' if the text is English, otherwise 'non-en'. Text:\n\n" + sample_text[:2000]
    try:
        out = invoke_anthropic_profile(PROFILE_IDS["anthropic_haiku_4_5"], q, max_tokens=5)
        return out.strip().lower().startswith("en")
    except Exception as e:
        log(f"LLM language check (Haiku) failed: {e}")
        try:
            out = invoke_anthropic_profile(PROFILE_IDS["anthropic_sonnet_4_5"], q, max_tokens=5)
            return out.strip().lower().startswith("en")
        except Exception as e2:
            log(f"LLM language check (Sonnet) failed: {e2}")
            return False

def corpus_is_english(text):
    t = (text or "").strip()
    if not t:
        return True
    try:
        lang = detect(t[:10000])
        log(f"Detected language via langdetect: {lang}")
        if str(lang).lower() == "en":
            return True
    except Exception as e:
        log(f"langdetect failed: {e}")
    first_chunk = chunk_text(t, limit=MAX_CHUNK_CHARS)[:1]
    if first_chunk:
        result = llm_is_english(first_chunk[0])
        log(f"LLM language detection result: {'English' if result else 'Non-English'}")
        return result
    return False

def translate_chunks_parallel(chunks, max_workers=10):
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        translated_chunks = list(executor.map(translate_chunk, chunks))
    return translated_chunks

def translate_chunk(text):
    try:
        pid = PROFILE_IDS["anthropic_haiku_4_5"]
        prompt = f"Translate this text to precise legal English. Preserve headings, numbering, and dates. No commentary.\n\n{text}"
        out = invoke_anthropic_profile(pid, prompt)
        if out:
            return out
    except Exception as e:
        log(f"Anthropic Haiku translation failed: {e}")
    try:
        pid = PROFILE_IDS["anthropic_sonnet_4_5"]
        prompt = f"Translate this text to precise legal English. Preserve headings, numbering, and dates. No commentary.\n\n{text}"
        out = invoke_anthropic_profile(pid, prompt)
        if out:
            return out
    except Exception as e:
        log(f"Anthropic Sonnet translation failed: {e}")
    return text

def process_file(file_path):
    print(f"Processing: {file_path.name}")
    raw = file_path.read_text(encoding="utf-8", errors="ignore")
    text = html_to_text(raw)
    chunks = chunk_text(text)
    print(f"  Translating {len(chunks)} chunks en parallèle")
    translated_chunks = translate_chunks_parallel(chunks, max_workers=8)
    final_text = "\n\n".join(translated_chunks)
    out_dir = Path(OUT_DIR)
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / f"{file_path.stem}.txt"
    out_file.write_text(final_text, encoding="utf-8")
    print(f"Saved: {out_file.name}")

def process_file(file_path):
    log(f"Processing: {file_path.name}")
    raw = file_path.read_text(encoding="utf-8", errors="ignore")
    text = html_to_text(raw)
    if corpus_is_english(text):
        log("Document is already in English. Skipping translation.")
        final_text = text
    else:
        log("Document is NOT in English. Starting translation process...")
        chunks = chunk_text(text)
        translated_chunks = []
        print(f"  Translating {len(chunks)} chunks")
        translated_chunks = translate_chunks_parallel(chunks, max_workers=10)
        final_text = "\n\n".join(translated_chunks)
        log("Translation completed successfully.")
    out_dir = Path(OUT_DIR)
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / f"{file_path.stem}.txt"
    out_file.write_text(final_text, encoding="utf-8")
    log(f"Saved translated text to {out_file.name}")

def main():
    files = get_files()
    log(f"Found {len(files)} files to process.")
    for file_path in files:
        process_file(file_path)
    log("All files processed.")

if __name__ == "__main__":
    main()


# # Chunk Cleaning

get_ipython().system('pip install nltk')


from pathlib import Path
from nltk.tokenize import wordpunct_tokenize

def clean_text(text):
    tokens = wordpunct_tokenize(text)
    tokens = [t for t in tokens if any(ch.isalnum() for ch in t)]
    return " ".join(tokens)

def process_translations(in_dir="out/translate", out_dir="out/processed"):
    in_path = Path(in_dir)
    out_path = Path(out_dir)
    out_path.mkdir(parents=True, exist_ok=True)
    for f in in_path.glob("*.txt"):
        raw = f.read_text(encoding="utf-8", errors="ignore")
        cleaned = clean_text(raw)
        (out_path / f.name).write_text(cleaned, encoding="utf-8")

if __name__ == "__main__":
    process_translations()


# # Results Section

# # Setup, config, helpers (files, chunking, Bedrock calls, JSON safety, dates)

# %% [markdown]
# ## 1) Setup, configuration & helper utilities
# - Constants, AWS Bedrock client, logging
# - Text chunking (sentence-aware up to MAX_CHUNK_CHARS)
# - Anthropic/Bedrock invocation with simple retries
# - Robust JSON cleaning/loading (handles ``` fences, smart quotes, extra text)
# - Date helpers + header date extraction heuristic

# %%
import json, time, re
from datetime import datetime
from pathlib import Path
import boto3
from botocore.config import Config
from concurrent.futures import ThreadPoolExecutor

AWS_REGION = "us-west-2"
IN_DIR = "out/processed"
OUT_DIR = "out/results"
MAX_CHUNK_CHARS = 4000

PROFILE_IDS = {
    "anthropic_haiku_4_5": "global.anthropic.claude-haiku-4-5-20251001-v1:0",
    "anthropic_sonnet_4_5": "global.anthropic.claude-sonnet-4-5-20250929-v1:0",
    "anthropic_sonnet_4": "global.anthropic.claude-sonnet-4-20250514-v1:0"
}

bedrock = boto3.client(
    "bedrock-runtime",
    region_name=AWS_REGION,
    config=Config(read_timeout=60, retries={"max_attempts": 3})
)

def log(msg): print(f"[LOG] {msg}")
def pct(n, d): return 0 if d == 0 else round(100 * n / d, 1)

def chunk_text(text, limit=MAX_CHUNK_CHARS):
    chunks, i = [], 0
    while i < len(text):
        end = min(i + limit, len(text))
        if end < len(text):
            for sep in [". ", ".\n", "! ", "? "]:
                k = text.rfind(sep, i, end)
                if k > i + 200:
                    end = k + len(sep)
                    break
        chunks.append(text[i:end].strip())
        i = end
    return [c for c in chunks if c]

def invoke_anthropic_profile(profile_id, user_text, max_tokens=2000, temperature=0.0):
    body = {
        "anthropic_version": "bedrock-2023-05-31",
        "max_tokens": max_tokens,
        "temperature": temperature,
        "messages": [{"role": "user", "content": [{"type": "text", "text": user_text}]}]
    }
    for attempt in range(3):
        try:
            resp = bedrock.invoke_model(modelId=profile_id, body=json.dumps(body))
            data = json.loads(resp["body"].read())
            return (data.get("content", [{}])[0].get("text") or "").strip()
        except Exception as e:
            log(f"LLM call failed (attempt {attempt+1}): {e}")
            time.sleep(1 + attempt)
    return ""

def _normalize_json_text(s):
    if not s: return ""
    s = (s.replace("\u2018","'").replace("\u2019","'")
           .replace("\u201C",'"').replace("\u201D",'"')
           .replace("\u00AB",'"').replace("\u00BB",'"')).strip()
    s = re.sub(r"^```(?:json)?", "", s, flags=re.IGNORECASE).strip()
    s = re.sub(r"```$", "", s).strip()
    m = re.search(r"[\{\[]", s)
    return s[m.start():] if m else s

def _find_balanced_json(s):
    # Fixed version: finds the first balanced JSON object/array substring
    s = s.strip()
    if not s: return ""
    start = None
    for i, ch in enumerate(s):
        if ch in "{[":
            start = i
            break
    if start is None: return ""
    stack = []
    in_str = False
    esc = False
    for j in range(start, len(s)):
        ch = s[j]
        if in_str:
            if esc:
                esc = False
            elif ch == "\\":
                esc = True
            elif ch == '"':
                in_str = False
            continue
        else:
            if ch == '"':
                in_str = True
                continue
            if ch in "{[":
                stack.append(ch)
            elif ch in "}]":
                if not stack:
                    return ""
                top = stack.pop()
                if (top == "{" and ch != "}") or (top == "[" and ch != "]"):
                    return ""
                if not stack:
                    return s[start:j+1]
    return ""

def safe_load_json(raw, expect_object=True):
    if not raw: return None
    txt = _normalize_json_text(raw)
    try:
        obj = json.loads(txt)
        if expect_object and not isinstance(obj, dict): return None
        return obj
    except Exception:
        pass
    seg = _find_balanced_json(txt)
    if not seg: return None
    try:
        obj = json.loads(seg)
        if expect_object and not isinstance(obj, dict): return None
        return obj
    except Exception:
        return None

MONTHS = {
    "january":1,"february":2,"march":3,"april":4,"may":5,"june":6,
    "july":7,"august":8,"september":9,"october":10,"november":11,"december":12
}
HEADER_RE = re.compile(
    r"(DIRECTIVE|REGULATION|DECISION)[^\n]{0,300}?\bOF\b\s+(\d{1,2}\s+[A-Za-z]+\s+\d{4})",
    re.IGNORECASE | re.DOTALL
)

def _to_iso_date(s):
    s = s.strip()
    if re.match(r"^\d{4}(-\d{2}(-\d{2})?)?$", s):
        y = int(s[:4])
        if 1950 <= y <= datetime.utcnow().year + 1:
            return s
        return ""
    m = re.match(r"^(\d{1,2})\s+([A-Za-z]+)\s+(\d{4})$", s)
    if m:
        d = int(m.group(1)); mon = MONTHS.get(m.group(2).lower()); y = int(m.group(3))
        if mon and 1 <= d <= 31 and 1950 <= y <= datetime.utcnow().year + 1:
            return f"{y:04d}-{mon:02d}-{d:02d}"
    return ""

def _header_date(text):
    head = text[:2000].replace("\u00A0"," ").replace("\u202F"," ")
    m = HEADER_RE.search(head)
    if not m: return ""
    return _to_iso_date(m.group(2)) or ""


# # Prompts, state model, per-chunk worker, parallel extraction

# %% [markdown]
# ## 2) Prompt builders, state merging, per-chunk worker, parallel extraction
# - JSON-only prompts for date refinement and state updates
# - State containers + merge logic (dedup, case-insensitive sort)
# - Worker function processes one chunk (date probe + state update)
# - `extract_from_chunks_parallel`: drives ThreadPoolExecutor over all chunks

# %%
def empty_state():
    return {
        "date": None,
        "jurisdiction_country": [],
        "sector": [],
        "activity": [],
        "regulatory_domain": [],
        "impact_type": [],
        "regulator_entity": []
    }

def empty_date_info():
    return {"date": None, "specificity": 0, "evidence_chunk": "", "locked": False, "law_header": ""}

def merge_state(state, update):
    for k in ["jurisdiction_country","sector","activity","regulatory_domain","impact_type","regulator_entity"]:
        seen = set(state.get(k) or [])
        for v in (update.get(k,[]) or []):
            v = (v or "").strip()
            if v:
                seen.add(v)
        state[k] = sorted(seen, key=lambda x: x.lower())
    return state

def build_state_prompt(law_id, prior_state_json, current_date, date_evidence, law_header, chunk_text):
    return f"""Return ONLY a JSON object with keys: "date","jurisdiction_country","sector","activity","regulatory_domain","impact_type","regulator_entity".
Rules:
- law_id is {law_id} and must NOT appear in output.
- Do not change "date". If you include "date", it MUST equal CURRENT_DATE or be a strictly more specific ISO refinement of the SAME year and month.
- For list fields, add unique strings supported by this chunk; do not remove prior values.

LAW_HEADER:
{law_header}

CURRENT_STATE:
{prior_state_json}

CURRENT_DATE_CONTEXT:
date: {current_date or ""}
evidence_chunk: {date_evidence or ""}

CHUNK:
{chunk_text}"""

def build_date_probe_prompt(law_id, law_header, current_date, current_specificity, chunk_text):
    return f"""Return ONLY JSON: {{"date":"","specificity":0,"is_stronger":false,"same_law":false,"confidence":0.0,"evidence":""}}.
- Consider ONLY dates that refer to THIS law (not citations to other instruments).
- same_law: true only if the chunk clearly ties the date to THIS law identified by law_id and header.
- confidence: 0..1 for that judgment.
- specificity: 3=YYYY-MM-DD, 2=YYYY-MM, 1=YYYY, 0=unknown.
- is_stronger: true only if same_law is true AND the candidate is more specific than CURRENT_DATE and same year.

law_id: {law_id}
law_header: {law_header}

CURRENT_DATE: {current_date or ""} (specificity={current_specificity})
CHUNK:
{chunk_text}"""

def call_json(prompt, expect_object=True, max_tokens=800):
    for pid in [
        PROFILE_IDS["anthropic_haiku_4_5"],
        PROFILE_IDS["anthropic_sonnet_4_5"],
        PROFILE_IDS["anthropic_sonnet_4"]
    ]:
        out = invoke_anthropic_profile(pid, prompt, max_tokens=max_tokens)
        obj = safe_load_json(out, expect_object=expect_object)
        if obj is not None:
            return obj
    return None

def process_single_chunk(args):
    idx, ch, law_id, date_info, state = args

    # deepcopy to isolate thread state
    import copy
    date_info = copy.deepcopy(date_info)
    state = copy.deepcopy(state)

    result = {}

    # Date probe (only if not locked)
    if not date_info.get("locked"):
        dprobe = call_json(build_date_probe_prompt(
            law_id,
            date_info.get("law_header", ""),
            date_info.get("date", ""),
            date_info.get("specificity", 0),
            ch
        ), expect_object=True, max_tokens=320)

        if isinstance(dprobe, dict):
            cand = _to_iso_date((dprobe.get("date") or "").strip())
            spec = int(dprobe.get("specificity") or 0)
            stronger = bool(dprobe.get("is_stronger"))
            same_law = bool(dprobe.get("same_law"))
            conf = float(dprobe.get("confidence") or 0.0)
            # Accept if clearly tied to the same law and confident
            if cand and same_law and conf >= 0.8:
                result["date"] = cand
                result["specificity"] = spec

    # State update
    sprompt = build_state_prompt(
        law_id,
        json.dumps({**state, "date": date_info.get("date", "")}, ensure_ascii=False),
        date_info.get("date", ""), date_info.get("evidence_chunk", ""),
        date_info.get("law_header", ""), ch
    )
    supd = call_json(sprompt, expect_object=True, max_tokens=800)
    result["state"] = supd if isinstance(supd, dict) else {}

    return idx, result

def extract_from_chunks_parallel(law_id, text):
    state = empty_state()
    date_info = empty_date_info()

    # Header date heuristic
    hd = _header_date(text)
    if hd:
        date_info["date"] = hd
        date_info["specificity"] = 3
        date_info["evidence_chunk"] = text[:2000]
        date_info["locked"] = True
        log(f"Date chosen (header) for {law_id}: {hd}")
    else:
        log(f"No header date found for {law_id}")

    chunks = chunk_text(text)
    if chunks:
        date_info["law_header"] = chunks[0][:1000]
    total = len(chunks)

    args_list = [(idx, ch, law_id, date_info, state) for idx, ch in enumerate(chunks, 1)]

    # Use a reasonable pool size to avoid throttling (tune as needed)
    max_workers = min(20, (os_cpu := __import__("os").cpu_count() or 4) * 5)
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        for idx, result in executor.map(process_single_chunk, args_list):
            # Date merge (prefer more specific)
            if "date" in result and result["date"]:
                if (not date_info["date"]) or (len(result["date"]) > len(date_info["date"])):
                    date_info["date"] = result["date"]
                    date_info["specificity"] = result.get("specificity", date_info["specificity"])
                    date_info["evidence_chunk"] = chunks[idx - 1][:2000]
                    date_info["locked"] = True
            # State merge
            if "state" in result and isinstance(result["state"], dict):
                state = merge_state(state, result["state"])
            log(f"Progress {law_id}: {idx}/{total} ({pct(idx, total)}%)")

    if date_info.get("date"):
        state["date"] = date_info["date"]
        log(f"Final date for {law_id}: {date_info['date']}")
    else:
        log(f"No date resolved for {law_id}")

    return state


# # I/O (CSV) & batch orchestration

# %% [markdown]
# ## 3) I/O (CSV) & batch orchestration
# - Writes one CSV per law with a single row (lists joined by semicolons)
# - Iterates over `out/processed/*.txt`, runs extraction, logs progress
# - Main guard to run the batch

# %%
def write_csv_row(path, row):
    path.parent.mkdir(parents=True, exist_ok=True)
    headers = ["law_id","date","jurisdiction_country","sector","activity","regulatory_domain","impact_type","regulator_entity"]
    def join(v):
        if v is None: return ""
        if isinstance(v, list): return ";".join(v)
        return str(v)
    with path.open("w", encoding="utf-8") as f:
        f.write(",".join(headers) + "\n")
        f.write(",".join([
            row.get("law_id",""),
            row.get("date","") or "",
            join(row.get("jurisdiction_country", [])),
            join(row.get("sector", [])),
            join(row.get("activity", [])),
            join(row.get("regulatory_domain", [])),
            join(row.get("impact_type", [])),
            join(row.get("regulator_entity", []))
        ]) + "\n")

def process_all():
    in_dir = Path(IN_DIR)
    out_dir = Path(OUT_DIR)
    out_dir.mkdir(parents=True, exist_ok=True)
    files = sorted(in_dir.glob("*.txt"))
    total = len(files)
    done = 0
    log(f"Found {total} files in {IN_DIR}")
    for f in files:
        law_id = f.stem
        log(f"Start processing {law_id}")
        text = f.read_text(encoding="utf-8", errors="ignore")
        # Use the defined parallel extractor
        state = extract_from_chunks_parallel(law_id, text)
        row = {"law_id": law_id, **state}
        write_csv_row(out_dir / f"{law_id}.csv", row)
        done += 1
        log(f"Completed {law_id} ({pct(done, total)}%)")
    log("All files processed.")

if __name__ == "__main__":
    process_all()
