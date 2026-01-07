import polars as pl

from main import (
	get_peptide_view,
	get_saved_sets_stats,
	get_searches,
)

search_ids = [
	37996,
	37997,
	37998,
	37999,
	38000,
	38001,
	39077,
	39078,
	39079,
	39080,
	39081,
	39082,
]

df = get_searches(search_ids=search_ids)

with pl.Config(tbl_rows=50, tbl_cols=-1, tbl_width_chars=10000, fmt_str_lengths=1000, fmt_table_cell_list_len=100):
	print(get_saved_sets_stats(search_id=search_ids[0]))
	print(get_saved_sets_stats(search_id=search_ids[-1]))

#	print(get_peptide_view(
#		search_id=search_ids[-1],
#		type="peptide",
#		set_bit=1,
#	))
