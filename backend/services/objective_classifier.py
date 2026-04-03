"""Campaign objective classification.

Classifies campaigns as 'awareness', 'conversion', or 'mixed' based on
media plan objective_format and campaign name keywords.
"""

AWARENESS_KEYWORDS = [
    "awareness", "reach", "frequency", "brand", "persuasion",
    "video views", "video completion", "audio", "impressions",
    "engagement", "display banner", "pre-roll", "mid-roll",
    "connected tv", "ctv", "ott", "branding",
]

CONVERSION_KEYWORDS = [
    "conversion", "conversions", "leads", "lead gen", "sales",
    "purchase", "acquisition", "app install", "sign up", "signup",
    "traffic", "clicks", "website visits", "landing page",
    "retargeting", "remarketing", "performance",
]


def classify_objective(
    objective_format: str | None = None,
    campaign_name: str | None = None,
) -> str:
    """Classify a campaign/line as 'awareness', 'conversion', or 'mixed'.

    Checks objective_format first (from media plan, most reliable),
    then falls back to campaign_name keyword matching.
    """
    for text in (objective_format, campaign_name):
        if not text:
            continue
        lower = text.lower()
        has_awareness = any(kw in lower for kw in AWARENESS_KEYWORDS)
        has_conversion = any(kw in lower for kw in CONVERSION_KEYWORDS)
        if has_awareness and has_conversion:
            return "mixed"
        if has_awareness:
            return "awareness"
        if has_conversion:
            return "conversion"

    return "mixed"


def classify_project(campaign_objectives: list[str]) -> str:
    """Determine project-level objective from a list of campaign objectives.

    Returns 'awareness' if all are awareness, 'conversion' if all conversion,
    otherwise 'mixed'.
    """
    if not campaign_objectives:
        return "mixed"
    unique = set(campaign_objectives)
    unique.discard("mixed")
    if unique == {"awareness"}:
        return "awareness"
    if unique == {"conversion"}:
        return "conversion"
    return "mixed"
