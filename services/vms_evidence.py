"""Conservative publication rules. A source title is evidence of a listing, not its outcome."""
import re
from urllib.parse import urlsplit


SOURCES = {
    "tga_consultations": ("TGA consultation", r"www\.tga\.gov\.au", r"/(?:news|resources)/consultations/[\w-]+/?", "Check the consultation scope, deadline and status; a proposal is not a final decision."),
    "artg": ("ARTG entry", r"www\.tga\.gov\.au", r"/resources/artg/\d+/?", "Check the entry, sponsor and product identity; a listing is not a new indication or a recall."),
    "cochrane": ("Cochrane review indexed in PubMed", r"pubmed\.ncbi\.nlm\.nih\.gov", r"/\d+/?", "Read the systematic review and check the population, intervention and certainty of evidence."),
    "pubmed": ("PubMed publication", r"pubmed\.ncbi\.nlm\.nih\.gov", r"/\d+/?", "Read the abstract and check the study population, design and endpoints before drawing conclusions."),
    "clinical_trials": ("ClinicalTrials.gov registration", r"clinicaltrials\.gov", r"/study/NCT\d{8}/?", "Check the registered interventions, outcomes and recruitment status. Registration alone does not establish efficacy."),
}
RELEVANT = re.compile(r"\b(?:dietary supplements?|herbal supplements?|nutraceuticals?|probiotics?|prebiotics?|creatine|ashwagandha|curcumin|omega[- ]3|(?:vitamin [a-ek]\d*|zinc|iron|magnesium|micronutrient|collagen|synbiotic) supplementation|vitamin [a-ek]\d* supplements?)\b", re.I)
ACTIONS = {
    "artg": "Check the ARTG entry, sponsor and product identity; a listing is not evidence of a new indication.",
    "adverse_events": "Review the original report and limitations. A report alone does not establish causation.",
    "tga_consultations": "Check the consultation scope, deadline and status; distinguish proposals from final decisions.",
    "efsa": "Check the original EFSA publication, its scope and evidence limitations.",
    "tga": "Read the TGA notice and verify the affected products, dates and status.",
    "fda": "Read the FDA notice and verify its scope and jurisdiction before acting.",
    "cochrane": "Check the review population, interventions and certainty of the evidence.",
    "semantic_scholar": "Check the original publication, methods and limitations before drawing conclusions.",
}


def classify(source, row):
    spec = SOURCES.get(source)
    title = " ".join((row.get("title") or "").split())
    url = urlsplit(row.get("url") or "")
    relevant = RELEVANT.search(title) or (source == "cochrane" and re.search(r"\b(?:turmeric|curcuma longa)\b", title, re.I))
    if not spec or row.get("authority") != source or not row.get("source_id") or not relevant:
        return None
    label, host, path, action = spec
    if url.scheme != "https" or not re.fullmatch(host, url.hostname or "") or not re.fullmatch(path, url.path):
        return None
    identity = {k:row[k] for k in ("source_id","authority","url","title","scraped_at") if k in row}
    return dict(identity, classification_method="source_fact_v1", source_label=source, severity="low",
                summary=f"{label}: {title}", event_type={"artg":"new_listing","tga_consultations":"consultation"}.get(source,"research"),
                signal_type={"artg":"product_listing","tga_consultations":"consultation","clinical_trials":"trial_registration"}.get(source,"research_publication"),
                why_it_matters={
                    "clinical_trials":"This is a study registration to monitor, not a clinical result or a regulatory decision.",
                    "artg":"The register identifies a product listing; it does not establish a change to permitted claims.",
                    "tga_consultations":"This is an opportunity to review a proposal, not notice of an adopted requirement.",
                }.get(source, "The publication identifies research to review. Its title alone does not establish efficacy, safety or a regulatory change."),
                recommended_action=action)


def historical_changes(row):
    source = row["source_label"] or row["authority"]
    edits = {}
    if source in SOURCES and row["summary"] == f"{SOURCES[source][0]}: {' '.join((row['title'] or '').split())}" and not classify(source, row):
        return {"is_noise": 1, "noise_reason": "Held for VMS review: no supported classification"}
    if not (row["summary"] or "").strip():
        result = classify(source, row)
        if not result:
            return {"is_noise": 1, "noise_reason": "Held for VMS review: no supported classification"}
        edits["summary"] = result["summary"]
    if not (row["recommended_action"] or "").strip():
        action = SOURCES[source][3] if source in SOURCES else ACTIONS.get(source)
        if action:
            edits["recommended_action"] = action
    if not (row.get("why_it_matters") or "").strip():
        fact = classify(source, row)
        if fact:
            edits["why_it_matters"] = fact["why_it_matters"]
    return edits
