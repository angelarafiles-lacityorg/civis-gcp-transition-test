"""
Test whether we can port Civis jobs over to GCP

Create simple workflow related to 311 data
Bring in GeoHub boundary of sorts
Simple aggregation
Export to S3?

Ex: https://github.com/CityOfLosAngeles/notebook-demos/blob/master/ibis-query-311.py

Google BigQuery tutorial: https://googleapis.dev/python/bigquery/latest/index.html
"""
import datetime
import geopandas
import ibis
import os
import pandas

from google.cloud import bigquery

CREDENTIAL = "./gcp-credential.json"
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = f'{CREDENTIAL}'

"""This way doesn't work - just use separate JSON and call it instead of putting into .env
credentials = service_account.Credentials.from_service_account_file(
    f"{CREDENTIAL}", scopes = ["https://www.googleapis.com/auth/cloud-platform"]
)
"""
client = bigquery.Client()

# Use ibis to construct SQL query
conn = ibis.bigquery.connect(
    project_id = 'ita-datalakepoc',
    dataset_id = 'publicwork_311'
)

""" OR, this also works
conn = ibis.bigquery.connect(
    dataset_id='ita-datalakepoc.publicwork_311')
"""

table = conn.table('ServiceRequestTopLevel')

WGS84 = "EPSG:4326"

COUNCIL_DISTRICTS_URL = (
    "https://services1.arcgis.com/tp9wqSVX1AitKgjd/"
    "arcgis/rest/services/MA_IRZ_MAP/FeatureServer/3/"
    "query?where=1%3D1&outFields=*&outSR=4326&f=json"
)


# Read in 311
def prep_311_data(expr):
    # First, subset columns
    keep_cols = ["SRNumber", "SRType", "CreatedDate", "Longitude", "Latitude"]
    expr = table[keep_cols]

    # There seems to be a date issue with ibis
    # Instead of casting it as a date, let's keep date as a string
    # Parse the string instead
    # We'll keep up to the last 2 year's of data and use pandas to further subset
    current_year = datetime.datetime.today().year
    prior_year = current_year - 1

    expr2 = expr[(expr.CreatedDate.contains(str(current_year))) | 
                 (expr.CreatedDate.contains(str(prior_year)))]

    # Remove specific request types
    expr3 = expr2[expr2.SRType != "Homeless Encampment"]
    
    # Compile shows the SQL statement
    print(ibis.bigquery.compile(expr3.limit(10)))

    # Execute the query and return a pandas dataframe
    df = expr3.execute(limit=None) 
    
    print("Successfully executed query")
    
    return df


def make_gdf_spatial_join_to_geography(df, GEOG_URL):
    TODAY_DATE = datetime.datetime.today().date()
    SIX_MONTHS_AGO = (TODAY_DATE - datetime.timedelta(days=183))
    
    # Change data types
    df = df.assign(
        CreatedDate = pandas.to_datetime(df.CreatedDate),
        Longitude = pandas.to_numeric(df.Longitude),
        Latitude = pandas.to_numeric(df.Latitude),
    )
    
    df2 = df[(df.CreatedDate >= pandas.to_datetime(SIX_MONTHS_AGO)) & 
            (df.CreatedDate <= pandas.to_datetime(TODAY_DATE))]
    
    df3 = df2[(df2.Longitude.notna()) & (df2.Latitude.notna())].reset_index(drop=True)
    
    # Make a gdf
    gdf = geopandas.GeoDataFrame(df3, 
                                 geometry = geopandas.points_from_xy(df3.Longitude, df3.Latitude),
                                 crs = WGS84
                                )
    

    # Import council districts dataset
    keep = ["District", "NAME", "geometry"]
    cd = geopandas.read_file(GEOG_URL)[keep]

    # Spatial join
    m1 = geopandas.sjoin(
        gdf.to_crs(WGS84), cd.to_crs(WGS84), how="inner", op="intersects"
    ).drop(columns="index_right")

    m1 = m1.reset_index(drop=True)

    return m1


def aggregate_by_category(df):

    # Groupby CD and 311 request type
    group_cols = ["District", "NAME", "SRType"]

    df = df.groupby(group_cols).agg({"SRNumber": "count"}).reset_index()

    return df


df = prep_311_data(table)
gdf = make_gdf_spatial_join_to_geography(df, COUNCIL_DISTRICTS_URL)
final = aggregate_by_category(gdf)
print(final)