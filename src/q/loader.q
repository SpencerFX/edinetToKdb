// ====================================================
// Load the csv as table
cols:`doc_id`filer_name_jp`filer_name_en`ticker`net_sales`operating_income`net_income`assets`liabilities`equity`roe`equity_ratio

types:"SSSSFFFFFFF"  / adjust based on actual columns

t:(types;enlist ",") 0: `:edinet_output/ipo_financials_2025_full.csv
// ====================================================
