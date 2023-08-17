
import psycopg2
from shapely.geometry import Polygon, LineString, Point, MultiPoint
import boto3
import os
import re
import googlemaps
import shapely.wkt
from shapely.geometry import Polygon, LineString, Point, MultiPoint
import ast
bucket = "container-zone-detection"
s3 = boto3.client('s3') 
s3_resource = boto3.resource('s3')
import pandas as pd

import geopandas as gpd
from geopandas import GeoDataFrame
import folium
from collections import Counter
from sentence_transformers import SentenceTransformer, util
from sklearn.metrics.pairwise import pairwise_distances
import io
session = boto3.session.Session(profile_name='nam_vnale')
s3 = session.resource('s3')
def read_csv(file_path,bucket_name = 'bucket-s3-co2-emission'):
    obj = s3.Object(bucket_name, file_path)
    data = obj.get()['Body'].read().decode('utf-8')
    df =  pd.read_csv(io.StringIO(data),index_col=False)
    return df

model = SentenceTransformer('xlm-r-bert-base-nli-stsb-mean-tokens')
conn = psycopg2.connect(
    user="reeferpulse",
    database="sea_inland",
    host="dev-sea-inland.cfmvowtqgii4.eu-west-3.rds.amazonaws.com",
    password="neb3rxj!ukv7bub3NHC"
)

cur = conn.cursor()
def filter_polygon_on_sea (wkt_str):
    cur.execute("select * from  bluepulse_is_onseaorwater_4326('"+wkt_str+"') ;")
    results = cur.fetchall()
    return results[0][0]

def distance_poly_plus_proche(lat,lon):
    cur.execute("SELECT find_distance_to_closest_sea_polygon("+str(lat)+", "+str(lon)+");")
    results = cur.fetchall()
    dist_to_sea = results[0][0]
    
    cur.execute("SELECT find_distance_to_closest_waterway_polygon("+str(lat)+", "+str(lon)+");")
    results = cur.fetchall()
    dist_to_waterway = results[0][0]
    return min(dist_to_sea,dist_to_waterway)


def zonecolors(counter):
    if 'OFFR' in counter['closest_terminal']:
        return {'fillColor': 'blue', 'color': 'blue'}
    elif 'OFFD' in counter['closest_terminal']:
        return {'fillColor': 'green', 'color': 'green'}
    elif 'BRTH' in counter['closest_terminal']:
        return {'fillColor': 'yellow', 'color': 'yellow'}
    elif'CUST' in counter['closest_terminal']:
        return {'fillColor': 'red', 'color': 'red'}
def zonename(counter):
    if 'OFFR' in counter['closest_terminal']:
        return "reefer"
    elif 'OFFD' in counter['closest_terminal']:
        return "storage"
    elif 'BRTH' in counter['closest_terminal']:
        return "loading quay"
    elif'CUST' in counter['closest_terminal']:
        return "custom office"
    


def get_addresses(coordinates_list, api_key):
    gmaps = googlemaps.Client(key=api_key)
    addresses = []

    for lat, lon in coordinates_list:
        result = gmaps.reverse_geocode((lat, lon))
        if result:
            addresses.append(result[0]['formatted_address'])
        else:
            addresses.append("Adresse non trouvée.")

    return addresses
def filter_dataframes_by_distance(df_with_address_quay, seuil):
    dist = []
    for i in range(len(df_with_address_quay.center_lat)):
        dist.append(distance_poly_plus_proche(df_with_address_quay.center_lat[i], df_with_address_quay.center_lon[i]))
    
    df_with_address_quay["distance min"] = dist
    
    df_kept = df_with_address_quay[df_with_address_quay['distance min'] < seuil]
    df_no_kept = df_with_address_quay[df_with_address_quay['distance min'] >= seuil]
    
    return df_kept, df_no_kept

def filter_dataframe_by_distance(df, d):
    df_with_address_not_cust_with_terminal_gps_keep = df[df["dist_to_point"] <= d]
    df_with_address_not_cust_with_terminal_gps_no_keep = df[df["dist_to_point"] > d]
    return df_with_address_not_cust_with_terminal_gps_keep,df_with_address_not_cust_with_terminal_gps_no_keep
 