import sys, os
"""
USAGE: 'python prospect.py <input csv filename>'
- input csv file should have headers which:
	- MUST include a 'company' column,
	- MAY include name, full name, first name, last name, title columns
	- Any other columns will be ignored

Creates file: <input file>_with_domains.csv
	- This is an intermediate file which includes the company domain, looked up via Google
	- If you want to recompute this file, you must delete it then re-run prospect.py
	- You can improve the accuracy of the search by manually correcting domains in this file then re-running
	- This step is slow because Google severely rate-limits it's API

Creates file: <input file>_prospect_results.csv
	- This is the final output
	- WARNING: if you re-run prospect.py, <input file>_prospect_results.csv WILL BE OVERWRITTEN

"""

infilename = sys.argv[1]

outfilename = os.path.splitext(infilename)[0]+'_with_domains.csv'

resultfilename = os.path.splitext(infilename)[0]+'_prospect_results.csv'

if not os.path.isfile(outfilename):
	os.system('python prep_prospects.py '+infilename+' '+outfilename)

os.system('python get_prospects.py '+outfilename+' '+resultfilename)
