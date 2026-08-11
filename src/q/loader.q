// ====================================================
// Load the csv as table
columns:`doc_id`filer_name_jp`filer_name_en`ticker`net_sales`operating_income`net_income`assets`liabilities`equity`roe`equity_ratio

types: "SPSSSSDSFFFFFFFFFFF";
dataDirectory: `:data/output/
fileToLoad: dataDirectory,();
stgFile: hsym `$(dataDirectory,fileToLoad);

t:(types;enlist ",") 0: stgFile;
// ====================================================
