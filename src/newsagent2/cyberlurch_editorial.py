from __future__ import annotations
import os, re
from datetime import datetime, timezone
from typing import Any, Dict, List, Set

STOPWORDS = {"the","a","an","and","or","to","of","for","in","on","with","from","at","by","is","are","was","were","this","that","it","as","be","om","och","det","den","att","som","en","ett","und","der","die","das","ein","eine","mit","zu","ist","im"}

def normalize_channel_name(name:str)->str:
    return re.sub(r"[^a-z0-9]","",(name or "").lower())

def _norm_set(vals:Set[str])->Set[str]:
    return {normalize_channel_name(v) for v in vals}

MAINSTREAM_NEWS_CHANNELS=_norm_set({"tagesschau","ZDFheute","VanessaWingardh"})
PRIORITY_DAILY_CHANNELS=_norm_set({"CanadianPrepper","preppernewsflash"})
CHRISTIAN_RESPECTFUL_CHANNELS=_norm_set({"AusGlaubenLeben","BeardedBibleBros","CBNnewsonline","CrossExamined","DLMChristianLifestyle","DLMChristianPerspective","DrJamesTour","DTBM","KapChatfield","offthekirb","RogerLiebiLIVE","WesHuff","WretchedNetwork","beholdisrael","SlingandStoneVideos","PredictiveHistory","Daily_Dose_Of_Wisdom"})
DRY_IRONY_ALLOWED_CHANNELS=_norm_set({"DoomDebates","THEALPHAPATHYT","Blakwoodz","RedactedNews","ThePoplarReport","CanadianPrepper","preppernewsflash"})


def infer_channel_tone_profile(channel_name:str, channel_topics:Dict[str,List[str]])->str:
    n=normalize_channel_name(channel_name)
    if n in MAINSTREAM_NEWS_CHANNELS: return "mainstream_news"
    if n in CHRISTIAN_RESPECTFUL_CHANNELS: return "christian_apologetics"
    topics=" ".join(channel_topics.get(channel_name,[])).lower()
    if any(k in topics for k in ["prophecy","endtimes"]): return "prophecy_endtimes"
    if n in PRIORITY_DAILY_CHANNELS or any(k in topics for k in ["prepper","survival"]): return "prepper_survival"
    if any(k in topics for k in ["finance","econom"]): return "finance"
    if n in DRY_IRONY_ALLOWED_CHANNELS: return "fringe_absurd"
    return "general"


def is_deep_dive_eligible(item:Dict[str,Any], channel_topics:Dict[str,List[str]])->bool:
    if item.get("content_status")=="metadata_only" or item.get("text_source")=="metadata_only": return False
    if normalize_channel_name(item.get("channel") or "") in MAINSTREAM_NEWS_CHANNELS and not item.get("allow_mainstream_deep_dive"):
        return False
    min_chars=max(200,int((os.getenv("CYBERLURCH_DEEPDIVE_MIN_TEXT_CHARS") or "2500").strip() or "2500"))
    text=(item.get("text") or "").strip()
    if len(text)<min_chars and len((item.get("transcript_full_summary") or "").strip())<max(500,min_chars//2): return False
    has_meaningful=any(bool((item.get(k) or "").strip()) for k in ["transcript_full_summary","transcript_key_points","text"])
    if not has_meaningful: return False
    return True


def extract_keywords(item:Dict[str,Any])->Set[str]:
    raw=" ".join([str(item.get("title") or ""),str(item.get("transcript_full_summary") or ""),str(item.get("transcript_key_points") or "")])
    toks=re.findall(r"[A-Za-zÄÖÜäöüßÅå]{3,}",raw)
    return {t.lower() for t in toks if t.lower() not in STOPWORDS}


def build_trend_clusters(items:List[Dict[str,Any]])->Dict[str,Any]:
    kws=[extract_keywords(it) for it in items]
    clusters=[]; boosted=0
    for i,it in enumerate(items):
        overlap=[]
        for j,other in enumerate(items):
            if i==j: continue
            inter=kws[i]&kws[j]
            if len(inter)>=2:
                overlap.append((j,inter))
        if overlap:
            union=set().union(*[x[1] for x in overlap])
            it["trend_cluster_size"]=1+len(overlap)
            it["trend_keywords"]=sorted(list(union))[:8]
            boosted+=1
    clusters_total=len([it for it in items if int(it.get("trend_cluster_size") or 0)>=2])
    return {"trend_clusters_total":clusters_total,"trend_boosted_items_total":boosted}


def score_cyberlurch_deep_dive_candidate(item:Dict[str,Any], all_items:List[Dict[str,Any]], channel_topics:Dict[str,List[str]], state:Dict[str,Any])->Dict[str,Any]:
    reasons=[]; score=0.0
    text_len=len((item.get("text") or "").strip())
    score += min(5.0, text_len/4000.0); reasons.append(f"text_length:{text_len}")
    tp=(item.get("transcript_processing") or "").strip()
    if tp in {"direct_full_transcript","chunked_full_transcript"}: score+=3; reasons.append("full_transcript_processing")
    elif "excerpt" in tp or item.get("transcript_was_truncated"): score+=1.5; reasons.append("excerpt_processing")
    elif (item.get("text_source") or "")=="description": score+=0.8; reasons.append("description_only")
    if normalize_channel_name(item.get("channel") or "") in PRIORITY_DAILY_CHANNELS: score+=2; reasons.append("priority_daily_channel")
    rec=item.get("published_at")
    if isinstance(rec,datetime):
        hrs=max(0.0,(datetime.now(timezone.utc)-rec).total_seconds()/3600.0)
        score+=max(0.0,1.0-min(1.0,hrs/48.0)); reasons.append("recency")
    cluster=int(item.get("trend_cluster_size") or 0)
    if cluster>=2: score+=min(2.5,0.6*cluster); reasons.append(f"trend_cluster:{cluster}")
    ch=item.get("channel") or ""
    topics=channel_topics.get(ch,[])
    score += 0.2*len(topics)
    if normalize_channel_name(ch) in MAINSTREAM_NEWS_CHANNELS: score-=8; reasons.append("mainstream_penalty")
    item["cyberlurch_deep_dive_score"]=round(score,3)
    item["cyberlurch_deep_dive_reasons"]=reasons
    return {"score":item["cyberlurch_deep_dive_score"],"reasons":reasons}
