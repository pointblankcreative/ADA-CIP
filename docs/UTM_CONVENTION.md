# ADA UTM Tagging Convention

**Status:** Canonical. This is the source of truth for how Point Blank tags paid-media
destination URLs so that GA4 traffic can be matched back to platform performance data.

**Last updated:** 2026-07-08

---

## 1. Why this exists

Two systems speak different languages:

- **GA4** knows *sessions, engagement, conversions* — keyed to string dimensions
  (`source` / `medium` / `campaign` / `term` / `content`).
- **Ad platforms** (Meta, Google, StackAdapt, …) know *spend, impressions, clicks* —
  keyed to numeric **IDs** (campaign ID, ad set ID, ad ID).

To compute cost-per-conversion / ROAS we need a **shared join key** present in both.
This convention manufactures that key: **stable IDs go in `utm_content` for the join;
human-readable names go in `campaign` / `term` for reporting legibility.**

Names for humans, IDs for machines.

---

## 2. The five parameters

| Parameter | Value | Purpose |
|---|---|---|
| `utm_source` | The platform, lowercased standard token — `facebook`, `google`, `linkedin`, `stackadapt`, `tiktok`, `snapchat` | Which platform |
| `utm_medium` | The channel standard — `cpc`, `paid_social`, `display`, `paid_video`, `native` | Which channel type |
| `utm_campaign` | **The top-level platform campaign name.** On StackAdapt this is the **campaign group** name. | Human-readable campaign, reporting |
| `utm_term` | **The ad set name** (Google: ad group; StackAdapt: line item). | Human-readable ad set, reporting |
| `utm_content` | **`{campaign_id}-{adset_id}-{ad_id}`** — the platform's own IDs. | ⭐ The machine join key |

> **On "creative" vs "ad":** at the URL-macro layer every platform emits the **ad ID**,
> not a separate creative-object ID (Meta's macro is `{{ad.id}}`; Google's `{creative}`
> is the ad ID). We standardise on **ad ID** as the third segment because it is uniform
> across platforms and always available in the link. Where a distinct creative object is
> needed, resolve it downstream from the ad ID — do not try to force it into the URL.

### Source / medium standard tokens

Keep these consistent — GA4 dimensions are case-sensitive, so always lowercase.

| Platform | `utm_source` | `utm_medium` (typical) |
|---|---|---|
| Meta (FB/IG) | `facebook` | `paid_social` |
| Google Ads (Search) | `google` | `cpc` |
| Google Ads (Display/YouTube) | `google` | `display` / `paid_video` |
| LinkedIn | `linkedin` | `paid_social` |
| StackAdapt | `stackadapt` | `native` / `display` |
| TikTok | `tiktok` | `paid_social` |
| Snapchat | `snapchat` | `paid_social` |

---

## 3. `utm_content` is the join key

`utm_content = {campaign_id}-{adset_id}-{ad_id}`, populated by each platform's **dynamic
macros** (you never type IDs by hand — the platform substitutes them at click time).

Because ADA already lands **both** GA4 conversions (`fact_ga4_daily`) **and** platform
spend in BigQuery, the join happens **in the warehouse**, not inside GA4. GA4 only has to
*carry* the key. `fact_ga4_daily.session_content` = `{campaign_id}-{adset_id}-{ad_id}` is
joined against the same IDs on the spend side.

- IDs never change on rename, never need URL-encoding, and join cleanly.
- Names (`campaign` / `term`) are for reporting readability only — never join on them.

---

## 4. Per-platform macro templates (copy-paste)

Set these as the URL parameters / tracking-template field on each platform. Values in
`{{ }}` / `{ }` / `__ __` are the platform's own macros and expand per ad automatically.

### Meta (Facebook / Instagram)
Ads Manager → ad level → **URL parameters** field:
```
utm_source=facebook&utm_medium=paid_social&utm_campaign={{campaign.name}}&utm_term={{adset.name}}&utm_content={{campaign.id}}-{{adset.id}}-{{ad.id}}
```

### Google Ads
Account/campaign **tracking template** (or final-URL suffix):
```
utm_source=google&utm_medium=cpc&utm_campaign={_campaignname}&utm_term={_adgroupname}&utm_content={campaignid}-{adgroupid}-{creative}
```
> Google note: `{campaignid}`, `{adgroupid}`, `{creative}` (= ad ID) are ValueTrack and
> resolve automatically. Campaign/ad-group *names* are not ValueTrack — either use custom
> parameters (`{_campaignname}`) set per campaign, or accept IDs only in campaign/term.
> If GA4 ↔ Google Ads are natively linked, `gclid` auto-tagging already imports Google —
> UTMs here are for warehouse-side consistency.

### Microsoft / Bing Ads
```
utm_source=bing&utm_medium=cpc&utm_campaign={CampaignName}&utm_term={AdGroupName}&utm_content={CampaignId}-{AdGroupId}-{AdId}
```

### StackAdapt
Tracking parameters on the campaign (campaign **group** → `utm_campaign`):
```
utm_source=stackadapt&utm_medium=native&utm_campaign={{CAMPAIGN_GROUP_NAME}}&utm_term={{CAMPAIGN_NAME}}&utm_content={{CAMPAIGN_ID}}-{{CAMPAIGN_ID}}-{{CREATIVE_ID}}
```
> StackAdapt's hierarchy is campaign group → campaign → ad/creative. Map **group →
> `utm_campaign`**, **campaign → `utm_term`**, **creative → the ad segment of content**.
> Verify the exact macro tokens against StackAdapt's current docs before launch.

### TikTok
```
utm_source=tiktok&utm_medium=paid_social&utm_campaign=__CAMPAIGN_NAME__&utm_term=__AID_NAME__&utm_content=__CAMPAIGN_ID__-__AID__-__CID__
```
> TikTok: `__CAMPAIGN_ID__` (campaign), `__AID__` (ad group), `__CID__` (ad).

### Snapchat
```
utm_source=snapchat&utm_medium=paid_social&utm_campaign={{campaign.name}}&utm_term={{adSquad.name}}&utm_content={{campaign.id}}-{{adSquad.id}}-{{ad.id}}
```

### LinkedIn ⚠️
LinkedIn has **no dynamic URL macros.** UTMs must be **hardcoded per creative**. Build the
string manually — put the real numeric campaign/campaign-group/creative IDs into
`utm_content` by hand, and the names into `utm_campaign` / `utm_term`:
```
utm_source=linkedin&utm_medium=paid_social&utm_campaign=<CAMPAIGN_NAME>&utm_term=<CAMPAIGN_NAME_ADSET>&utm_content=<CAMPAIGN_GROUP_ID>-<CAMPAIGN_ID>-<CREATIVE_ID>
```
This is the highest-risk platform for tagging errors — use a URL builder and double-check.

### DOOH (Perion / Hivestack)
**Out of scope.** No clicks, no landing page, no UTMs. Measured by impressions / lift.

---

## 5. Rules & gotchas

1. **Lowercase `source` and `medium`.** GA4 string dimensions are case-sensitive.
2. **Never join on names.** `campaign` / `term` are display-only; the join is always
   `utm_content` IDs ↔ platform IDs.
3. **Encoding:** names contain spaces / punctuation — let the platform URL-encode them and
   expect some messiness. IDs stay clean, which is why the join rides on `content`.
4. **"Creative" = ad ID** across every platform (see §2). Keeps the join uniform.
5. **LinkedIn is manual** — no macros. Budget tagging discipline there.
6. **DOOH is excluded** — no click surface.
7. **Optional hardening — `utm_id`:** consider also setting `utm_id={campaign_id}`. It maps
   to GA4's native *"Manual campaign ID"* dimension and is the key GA4's own cost-data
   import matches on — cheap insurance if we ever want GA4-side matching.

---

## 6. Relationship to the current codebase (⚠️ migration note)

The GA4 attribution code shipped **before** this convention and assumes a **different**
`utm_content` format. Adopting this doc requires a code change — it is **not** purely additive.

- **Shipped today** (`backend/routers/ga4.py`, `ingestion/transformation/ga4_transform.py`):
  `utm_content = {project}-{campaign_id}-{adset_id}-{ad_id}` — note it **leads with the
  project code** and the auto-attribution layer matches `session_content LIKE '{project}-%'`.
- **This convention:** `utm_content = {campaign_id}-{adset_id}-{ad_id}` — **no project prefix.**

**Consequence:** once tags emit the new format, the project-code-prefix match on the content
layer stops matching. Project attribution then relies on the **campaign-name layer**
(`LEFT(session_campaign, LEN(code)) = code`) — which still works, because `utm_campaign` is
the platform campaign name and every PB campaign name starts with the `YYNNN` project code
(see CLAUDE.md §5). But the content-layer logic in `ga4.py` must be **rewritten** to:

1. stop treating `session_content` as a project-scoped key, and
2. parse/join `session_content` as `{campaign_id}-{adset_id}-{ad_id}` against platform spend.

Until that code change ships, this doc is the **target** convention; the live attribution
still expects the old project-prefixed format. Do not roll out the new tags to production
campaigns before the `ga4.py` content-layer rewrite lands, or project attribution on the
content layer will silently fall through to the campaign-name fallback.
