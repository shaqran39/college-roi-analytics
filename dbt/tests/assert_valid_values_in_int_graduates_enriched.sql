-- Custom test: returns any rows that break the rules below. 0 rows = pass.
-- 1. roi_band must agree with the dataset's own ROI flags
-- 2. ratios can't be negative
-- 3. the data dictionary's formulas must hold. Money columns are each rounded
--    to the nearest $100 in the source, so allow one $100 rounding step
select *
from {{ ref('int_graduates_enriched') }}
where (roi_band = 'high') <> is_high_roi
   or (roi_band = 'negative') <> (not is_positive_roi)
   or (roi_band = 'low' and net_roi_usd >= 200000)
   or (roi_band = 'medium' and net_roi_usd < 200000)
   or debt_to_annual_earnings_ratio < 0
   or payback_years < 0
   or abs(hs_baseline_10yr_usd + added_earnings_10yr_usd - earnings_10yr_usd) > 100
   or abs(added_earnings_10yr_usd - net_cost_usd - net_roi_usd) > 100
