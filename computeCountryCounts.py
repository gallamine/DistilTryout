# This function can run in parallel to parse the data and return the Country/Hour violations
def computeCountryCounts(credentials, bucket_name, location):
	# Open S3 bucket at location and load data. Return the total lines
	# the total count (sum of "total" column), and the histogram of country
	# totals as a dictionary. e.g. {'US':100,'DE':10,...}

	# If the bucket doesn't exist or if the credentials are wrong, return Null

	# Connect via Boto
	# Open bucket
	# Import buck into Pandas dataframe

	# Run summary statistics

	import boto
	from boto.s3.connection import OrdinaryCallingFormat
	from StringIO import StringIO
	import pandas as pd

	conn = boto.connect_s3(credentials['key'], credentials['secret'],calling_format=OrdinaryCallingFormat())

	bucket = conn.get_bucket(bucket_name)

	data_key = bucket.get_key(location)
	filepath_or_buffer = StringIO(data_key.get_contents_as_string())
	d = pd.read_csv(filepath_or_buffer,"", quotechar="",header=None,names=["prim_id","id","distil_domain_id","user_agent","ref","ah","country","violations","total"])

	bot_idx = (d["violations"] > 0) & (d["violations"] < 16384) 
	N_violations = bot_idx.sum()
	
	if N_violations > 0:
		hour_country_group = d[bot_idx].groupby(["country","ah"])		# Group data by country and hour
		total_lines = N_violations       								# Total violations
		total_count = d["total"][bot_idx].sum()							# Violation counts
		country_hist = hour_country_group["total"].sum().to_dict()		# Country/Hour violations
	else:
		total_lines = 0
		total_count = 0
		country_hist = {}
		
	return total_lines, total_count, country_hist