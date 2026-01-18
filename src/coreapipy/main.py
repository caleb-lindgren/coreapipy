import io
import json
import polars as pl
import requests

default_server = "bison.med.harvard.edu"
#default_server = "localhost:8443"

with open(".username", "r") as username_file:
	username = username_file.readline().split("#")[0].strip()

with open(".api_key", "r") as api_key_file:
	api_key = api_key_file.readline().split("#")[0].strip()

session = requests.Session()
session.auth = (username, api_key)

# Always raise for status
session.hooks = {
    "response": lambda resp, *args, **kwargs: resp.raise_for_status(),
}

def get_search_info(search_id, server):

	base_url = f"https://{server}/gfy/www/modules/api/v1"
	url = f"{base_url}/search/{search_id}"
	resp = session.get(url)
	search_info = resp.json()
	return search_info

def get_searches(
	search_ids=None,
	list_to_str=True,
	server=default_server,
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
	server=default_server,
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
	server=default_server,
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

def get_raws_paths(server=default_server):
	
	base_url = f"https://{server}/gfy/www/modules/api/v1"
	url = f"{base_url}/raw"

	resp = session.get(url)
	return resp.json()

def post_search(
	workflow_path,
	raws,
	server=default_server,
):

	with open(workflow_path, "r") as workflow_file:
		workflow = json.load(workflow_file)

	workflow["items"][0]["parameters"]["raws"] = raws

	workflow_str = json.dumps(workflow["items"])
	data = {
		"items": workflow_str,
	}

	base_url = f"https://{server}/gfy/www/modules/api/v1"
	url = f"{base_url}/bulk_queue"

	resp = session.post(url, data=data)
	return resp

def post_raw(path, name, server=default_server):

	data = {
		"name": name,
	}

	files = {
		"file": open(path, "rb"),
	}

	base_url = f"https://{server}/gfy/www/modules/api/v1"
	url = f"{base_url}/raw"

	resp = session.post(url, data=data, files=files)
	return resp

def get_search_params(type, path=None, server=default_server):

	params = {
		"type": type,
	}

	if path is not None:
		params["path"] = path

	base_url = f"https://{server}/gfy/www/modules/api/v1"
	url = f"{base_url}/search_param"

	resp = session.get(url, params=params)
	return resp.text

def get_job(id, server=default_server):

	base_url = f"https://{server}/gfy/www/modules/api/v1"
	url = f"{base_url}/job/{id}"

	resp = session.get(url)
	return resp

def get_protein_map(id, server=default_server):

	base_url = f"https://{server}/gfy/www/modules/api/v1"
	url = f"{base_url}/protein_assembler/{id}"

	resp = session.get(url)
	df = pl.read_csv(io.StringIO(resp.text), separator="\t")
	return df
