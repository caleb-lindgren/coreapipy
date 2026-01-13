import io
import polars as pl
import requests


with open(".username", "r") as username_file:
	username = username_file.readline()

with open(".api_key", "r") as api_key_file:
	api_key = api_key_file.readline()

session = requests.Session()
session.auth = (username, api_key)

def get_search_info(search_id, server):

	base_url = f"https://{server}/gfy/www/modules/api/v1"
	url = f"{base_url}/search/{search_id}"
	resp = session.get(url)
	search_info = resp.json()
	return search_info

def get_searches(
	search_ids=None,
	list_to_str=True,
	server="bison.med.harvard.edu",
):

	if search_ids is None:		
		base_url = f"https://{server}/gfy/www/modules/api/v1"
		url = f"{base_url}/search"
		resp = session.get(url)
		search_ids = [search["search_id"] for search in resp.json()]

	info_list = [
		get_search_info(search_id, server=server)
		for search_id in search_ids
	]
	print(info_list)
	df = pl.DataFrame(info_list)

	if list_to_str:
		df = df.with_columns(
			pl.col.saved_set_ids.cast(pl.List(pl.String)).list.join(",")
		)

	return df

def get_saved_sets_stats(
	search_ids,
	filter_nulls=False,
	filter_errors=False,
	server="bison.med.harvard.edu",
):

	stats_list = []
	for search_id in search_ids:
		base_url = f"https://{server}/gfy/www/modules/api/v1"
		url = f"{base_url}/saved_sets_stats/{search_id}"
		resp = session.get(url)
		stats_list.extend(resp.json())

	df = pl.DataFrame(stats_list)

	if filter_nulls:
		df = df.filter(~pl.col("Set bit").is_null())

	if filter_errors:
		df = df.filter(~(pl.col("Saved set") == ""))

	return df

def get_peptide_view(
	search_id,
	type,
	set_bit=None,
	columns=None,
	lda=None,
	schema=None,
	server="bison.med.harvard.edu",
):

	base_url = f"https://{server}/gfy/www/modules/api/v1"
	url = f"{base_url}/peptide_view/{search_id}"
	params = {
		"type": type,
		"set_bit": set_bit,
		"columns": columns,
		"lda": lda,
		"schema": schema,
	}

	resp = session.get(url, params=params)
	df = pl.read_csv(io.StringIO(resp.text), separator="\t")

	return df
