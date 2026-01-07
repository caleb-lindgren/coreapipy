import io
import polars as pl
import requests

server = "bison.med.harvard.edu"
base_url = f"https://{server}/gfy/www/modules/api/v1"

with open("username", "r") as username_file:
	username = username_file.readline()

with open("api_key", "r") as api_key_file:
	api_key = api_key_file.readline()

session = requests.Session()
session.auth = (username, api_key)

def get_search_info(search_id):

	url = f"{base_url}/search/{search_id}"
	resp = session.get(url)
	search_info = resp.json()
	return search_info

def get_searches(search_ids=None):

	if search_ids is None:
		url = f"{base_url}/search"
		resp = session.get(url)
		search_ids = [search["search_id"] for search in resp.json()]

	info_list = [
		get_search_info(search_id)
		for search_id in search_ids
	]
	df = pl.DataFrame(info_list)
	return df

def get_saved_sets_stats(search_id):

	url = f"{base_url}/saved_sets_stats/{search_id}"
	resp = session.get(url)
	df = pl.DataFrame(resp.json())
	return df

def get_peptide_view(
	search_id,
	type,
	set_bit=None,
	columns=None,
	lda=None,
	schema=None,
):

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
