# Process the hafrasha permit-use queue (Claude-vision step)

An approved plan can impose a **הפרשה מבונה** — public floor area inside a private
building — and describe it only as "מבנים ומוסדות ציבור" or "*בתיאום עם מחלקת מבני
ציבור*". That says an allocation exists but not what it is, and for plans already at
permit stage the space may already be built. The **גרמושקה** in the permit file is
where the use is finally named.

`build_hafrash_read_queue.py` decides who needs this and `prep_hafrash_gram.py`
downloads and renders the drawings. Reading the use off an architectural sheet is
the one step that needs Claude vision — this is how a session does it. It mirrors
`process_floor_queue.md`.

## Steps

1. **Prep.** `py build_hafrash_read_queue.py` then `py prep_hafrash_gram.py`.
   Read `C:\ORANIM\_hafrash_gram\_index.json`; work the entries with
   `status == "rendered"`.

2. **Spawn one vision agent per plan** (Agent tool, `general-purpose`, in parallel,
   up to ~6 at a time). Give each agent the prompt below. Each writes its result to
   `C:\ORANIM\_hafrash_agent_out_<taba>.json`.

3. **Merge + publish + dequeue:** `py hafrash_read_merge.py --push`

## The agent prompt

> You are reading an approved building-permit **גרמושקה** to find the actual chosen
> use of a **הפרשה מבונה** — public floor area that plan **101-{taba}** imposed on this
> lot but described only generically as `{hafrash_prg}`.
>
> **Expected:** ~**{hafrash_sqm} מ"ר**, on מגרש **{hafrash_lots}**. Permit tik **{tik}**
> ({doc_kind}) — *"{permit_descr}"*.
>
> **Files** are in `C:\ORANIM\_hafrash_gram\{taba}\`, listed in that plan's entry of
> `C:\ORANIM\_hafrash_gram\_index.json`:
> - `_contact.png` — every sheet as one tile grid. Start here to pick the sheet.
> - `*_pNN.png` — whole sheets. **Locating only.** At this scale an 8 pt Hebrew label
>   is ~2 px tall; anything you think you read here you invented.
> - To actually read: `py C:\ORANIM\zoom_hafrash.py {taba} --sheet NN` renders that
>   sheet's panels at a legible scale and prints their filenames + bboxes.
>   `--panel J` for one panel, `--bbox x0 y0 x1 y1 [--scale S]` for an exact region
>   (fractions of the whole sheet). `--list` shows the sheet inventory.
>
> Some sheets are enormous — 101-1037670's is 42,519 pt wide — and `--sheet` will
> refuse rather than emit 129 tiles. For those, localise then zoom:
> `--strips 8` renders eight equal columns at a scale good enough to see *where* the
> area tables, legends and floor plans are (not to read them), then come back with
> `--bbox`. Strip *k* of *n* spans sheet-x `k/n … (k+1)/n`, so a feature at 30% across
> strip 7 of 8 is at sheet-x `0.875 + 0.125*0.30 ≈ 0.91`.
>
> **Where to look, in order:**
> 1. **תשריט חישוב שטחים** — the colour-coded area plans (usually the first sheets).
>    Each use gets a colour and a legend entry; a public allocation shows up as its
>    own colour with a named legend row.
> 2. **טבלת שטחים** — a row whose use column reads `שטח ציבורי` / `מבנה ציבור` /
>    `שטח לצורכי ציבור` / a concrete use (`מועדון`, `גן ילדים`, `מעון`, `בית כנסת`,
>    `מקווה`, `מרפאה`, `דירת רווחה`), with a מ"ר figure near {hafrash_sqm}.
> 3. **תכניות קומה** — a room or zone labelled with such a use, usually on קומת קרקע
>    or קומה א', often hatched or coloured differently.
> 4. **חתך / חזית** — a labelled band.
>
> **Step A — validate the permit.** Find the `בקשה להיתר` / `רשות רישוי` data table
> (it is on one of the sheets even when `cover_png` is absent) and read
> **תאור הבקשה** / **ניתן בזה היתר ל**. If the permit's subject is only
> `חפירה ומילוי` / `דיפון` / `הריסה` / `שימוש חורג`, this file cannot answer the
> question → return `outcome:"wrong_permit"` with the quoted text, and stop.
>
> **Step B — validate the plan and the lot.** The same table carries **תב"ע** and
> **מגרש**. If the תב"ע does not match `101-{taba}` → `outcome:"wrong_plan"`. If it is
> unreadable, set `taba_match:null` and cap confidence at `medium`. If the plan has
> several hafrasha lots and the sheet's מגרש is not one of `{hafrash_lots}`, return
> `not_found` — do not attribute another lot's kindergarten to this one.
>
> **Step C — evidence.** Every reading must cite: the PNG filename, the panel or table
> it came from, the **exact Hebrew string as printed** (no paraphrase, no spelling
> normalisation), and the מ"ר figure as printed.
>
> **Confidence — apply literally:**
> - `high` — a printed use label read verbatim **from a zoom render** (`*_z*.png`),
>   **and** a מ"ר figure within ±15% of {hafrash_sqm}, **and** the תב"ע matches.
> - `medium` — a clear printed label, but the מ"ר is missing or off by >15%, or the
>   תב"ע could not be verified.
> - `low` — inferred from hatching, colour, position, or the permit's text description
>   rather than a printed label. A `low` read is **not published**; it only flags the
>   plan for manual review.
>
> **If a label is visible but not readable, say so and name the region** (sheet, bbox
> fractions) so it can be re-rendered larger. **Never guess a label.**
>
> **`allocation_dropped`** means the sheet affirmatively shows the public area was not
> built — the area table reconciles with no public row, or a note says the הפרשה was
> commuted or relocated. Absence of evidence is **`not_found`**. Getting this wrong
> turns "we didn't look hard enough" into a published claim that a public allocation
> vanished.
>
> **Write JSON only** to `C:\ORANIM\_hafrash_agent_out_{taba}.json`:
> ```json
> {"<taba>": {
>   "outcome": "found | not_found | allocation_dropped | wrong_permit | wrong_plan | illegible",
>   "confidence": "high|medium|low",
>   "label_he": "מבנה ציבור - מועדון נוער",
>   "uses": ["מועדון"],
>   "sqm_read": 118, "sqm_expected": 1631, "sqm_match": false,
>   "lot": "201", "taba_on_sheet": "101-0571190", "taba_match": true,
>   "permit_subject": "בנין מגורים חדש הכולל קומת מסחר ושטחים ציבוריים",
>   "evidence": [{"png": "2017_0384.02_d39_p03_z03b.png", "sheet": 3,
>                 "panel": "טבלת בקשה להיתר",
>                 "quote_he": "שטח ציבורי  118.4 מ\"ר",
>                 "bbox_frac": [0.91, 0.16, 1.0, 0.33]}],
>   "sheets_examined": [3, 4, 5],
>   "notes": ""
> }}
> ```

## Calibration

Before trusting any of this, run it blind on the six plans whose answer
`hafrasha_delivery.json` already holds:

```
py prep_hafrash_gram.py --calibrate
```

`--calibrate` deliberately builds its queue **without** reading
`hafrasha_delivery.json`, so the agents cannot see the answer. Then compare with
`py hafrash_read_merge.py --calibrate-report`. The gate is **80% of the cases the
book actually answers** (minimum 3), not a fixed 5-of-6: 101-1249358 has no licensing
file at all, and placeholder book rows — a generic "הפרשה מבונה - חברה/קהילה/רווחה"
with built_sqm 0 — are excluded, since scoring against those measures agreement with
a guess. Nothing from the real queue is published until the gate passes.
