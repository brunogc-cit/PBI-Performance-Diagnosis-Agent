# Finding Suppression Rules

Rules learned from human architect feedback on the Trade analysis (2026-04-13).
The agent MUST apply these during Step 7 synthesis to avoid non-actionable recommendations.

These rules encode domain knowledge about what a senior data architect considers actionable
versus noise. They improve the signal-to-noise ratio of the Action Register.

---

## SUPPRESS — Never include in Action Register or Synthesis findings

### S1: Query Reduction Report Settings (V05)

- **Rule**: V05 (Auto-refresh or query reduction settings)
- **Applies to**: "Enable Apply buttons", "Disable cross-highlighting", query reduction settings
- **Reason**: These are organisation-level policy decisions, not performance fixes. The report author cannot unilaterally change these settings — they require alignment across all reports and teams. The performance gain is marginal compared to the effort of changing org-wide policy.
- **Action**: SUPPRESS from Action Register entirely. Do not promote to Synthesis findings or implementationRoadmap. The V05 findings still appear in the Visual Analysis HTML section table as informational context.

### S2: Embedded Images (V08)

- **Rule**: V08 (Large embedded images)
- **Applies to**: All embedded image findings across all reports
- **Reason**: Negligible performance impact in DirectQuery-dominant models. Image size is irrelevant compared to query execution cost — a 200 KB image adds <50ms to page load, while a single DirectQuery can take 15+ seconds. This is noise that distracts from real performance issues.
- **Action**: SUPPRESS from Action Register entirely. Do not promote to Synthesis. The V08 findings still appear in the Visual Analysis HTML section table as informational context.

### S3: Non-Performance DAX Style Rules

- **Rules**: MISSING_FORMAT_STRING, UNQUALIFIED_COLUMNS, HARDCODED_VALUES
- **Applies to**: DAX Audit findings with zero engine-level performance impact
- **Reason**: These are code quality/style issues, not performance. The architect focuses exclusively on patterns that affect SQL generation, scan volume, and query cost. Format strings affect display only. Unqualified columns affect readability only. Hardcoded values affect maintainability only.
- **Action**: SUPPRESS from Action Register. Show only in BPA/DAX Audit detailed table (informational), never promote to synthesis findings or roadmap.

### S4: Non-Performance Engineering Style Rules

- **Rules**: E13 (Hardcoded magic numbers in SQL)
- **Applies to**: SQL style rules with no query execution impact
- **Reason**: Maintainability concern, not performance. The `performanceImpact` field for E13 is already `quality`, confirming no latency/cost impact.
- **Action**: SUPPRESS from Action Register. Show in Engineering BPA table only.

### S5: New Pre-Aggregation Model Creation

- **Applies to**: Any synthesis recommendation to CREATE a brand-new dbt model for pre-aggregation (e.g., "Create weekly_trade_summary", "Create narrow serve view")
- **Reason**: Adding new dbt models increases pipeline complexity, maintenance burden, and CI/CD overhead. The architect prefers optimising existing serve views (materialise as materialised_view, add WHERE filters, narrow column selection) over creating new models.
- **Action**: SUPPRESS from Action Register. Instead, note as a "Future consideration" in the relevant finding's `tradeoffs` field if the optimisation would genuinely benefit from a new model. Never present new model creation as a primary recommendation.

---

## DOWNGRADE — Include in detail tables but do not promote to Action Register

### D1: BPA Detailed Rule Violations (bulk counts)

- **Rules**: All BPA rules when presented as "Fix N violations" bulk items
- **Reason**: The architect did not review these individually — they are useful as reference data but not actionable as standalone tasks. When a BPA rule violation is already covered by a more specific Synthesis finding (e.g., FILTER_ALL_ANTIPATTERN is covered by a DAX refactoring finding), the BPA bulk item adds duplication.
- **Action**: Keep in BPA section tables for completeness. Do NOT create individual Action Register entries for BPA bulk violations that are already covered by a Synthesis finding. Avoid duplication between Synthesis actions and BPA detail actions.

---

## Action Classification Guidance

When building the implementationRoadmap, classify actions by confidence level:

- **Accept** (high confidence): Clear performance impact, well-defined implementation, no business-logic dependency. The agent assigns these directly.
- **Validate** (needs investigation): Performance impact depends on runtime behaviour (e.g., Dual→Import switches, clustering column choices, WHERE filters on dimension tables). Mark with "Validate before implementing" note.
- **Propose** (needs stakeholder input): Report design changes (page splits, visual count reduction, matrix column cuts) that affect user experience and require business user alignment. Mark with "Propose to report owner" note.
