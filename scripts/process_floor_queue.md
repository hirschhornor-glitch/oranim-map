# Process the floor-read queue (Claude-vision step)

`enrich_changed_plans.py` prepares everything automatically (downloads the נספח
בינוי, renders the cross-section PNGs) and lists each plan that needs a floor
read in `floor_read_queue.json`. Reading the floors off an architectural section
is the one step that needs Claude vision — this is how a Claude session does it.
It mirrors the proven broad run (76 plans, parallel vision agents).

## Steps

1. **Read the queue:** open `C:\ORANIM\floor_read_queue.json`. Each entry is
   `{taba, plan_name, png_paths: [...], queued_at}`.

   The `png_paths` are candidate drawing-sheet thumbnails + a `_contact.png`
   tile sheet (from `prep_binui_thumbs.py`). They're low-res for *identifying*
   the right sheet; for reading exact floor labels, render that sheet at high
   DPI on demand with pypdfium2 (proven technique — see the floor-allocations
   memory): `pdfium.PdfDocument(r"plan_documents/101-<taba>/_<sheet>.pdf")[0].render(scale=6.5).to_pil()`.

2. **Spawn one vision agent per plan** (Agent tool, `general-purpose`, in
   parallel — up to ~6 at a time). Give each agent the plan's `png_paths` and
   this instruction:

   > Read these building-appendix cross-section / floor-plan PNGs for plan
   > 101-<taba>. For each **built public allocation** (גן ילדים / מעון / בית ספר /
   > בית כנסת / מקווה / קהילה / רווחה / תרבות / מבנה ציבור) identify which floor it
   > sits on. On a sloped/mixed site read the **absolute level from the levels
   > column** rather than counting floor numbers, and remember a פודיום is **not**
   > ground floor (commerce is usually below it). Return JSON only:
   > `{"<taba>": {"source_doc": "...", "source_type": "colored_section|elevation_plus_floorplans|floorplan|text_only|none_found", "confidence": "high|medium|low", "allocations": [{"use": "...", "floor_start": "5|קרקע|-2", "floor_label": "...", "confidence": "high|medium|low", "note": "..."}]}}`
   > If no usable section exists, return `{"<taba>": {"source_type": "none_found", "allocations": []}}`.

   Have each agent write its result to `C:\ORANIM\_floor_agent_out_<taba>.json`
   (or return it and you write the file).

3. **Merge + publish + dequeue:**
   ```
   py floor_read_merge.py
   ```
   This folds all `_floor_agent_out_*.json` into `floor_allocations.json` (via
   `add_floor_alloc.py`, both the root and `oranim-app/data/` copies) and removes
   the handled plans from `floor_read_queue.json`.

4. **Commit + push** `oranim-app/data/floor_allocations.json` (see the git
   divergence note in the floor-allocations memory — fetch → rebase → rebuild
   app.js if it conflicts).

## Optional: schedule it hands-off
Run as a local scheduled Claude session (Windows Task Scheduler → `claude -p`)
right after the local `enrich_changed_plans.py` run, so detect → enrich → floor
read all complete in one nightly chain. The downloads and the vision read both
need the local machine, so keep this on the workstation, not the cloud cron.
