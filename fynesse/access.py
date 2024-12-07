from .config import *

"""These are the types of import we might expect in this file
import httplib2
import oauth2
import tables
import mongodb
import sqlite"""

# This file accesses the data

"""Place commands in this file to access the data electronically. Don't remove any missing values, or deal with outliers. Make sure you have legalities correct, both intellectual property and personal data privacy rights. Beyond the legal side also think about the ethical issues around this data. """


def data():
    """Read the data from the web or local file, returning structured format such as a data frame"""
    raise NotImplementedError


# from .config import *

"""These are the types of import we might expect in this file
import httplib2
import oauth2
import tables
import mongodb
import sqlite"""
import pymysql
import requests
import csv
import warnings
import osmnx as ox
import zipfile
import io
import pandas as pd

def bounding_extract_region_data(conn, region_name, latitude, longitude, distance_km):
    cur = conn.cursor()
    print(
        f"Selecting data for transactions since in the region {region_name} centered at {latitude}, {longitude}"
    )
    cur.execute(
        """
    SELECT *
    FROM housing_data
    WHERE latitude BETWEEN %s AND %s
      AND longitude BETWEEN %s AND %s;
      """,
        (
            latitude - distance_km / 2,
            latitude + distance_km / 2,
            longitude - distance_km / 2,
            longitude + distance_km / 2,
        ),
    )
    rows = cur.fetchall()

    csv_file_path = f"{region_name}_housing_data.csv"
    with open(csv_file_path, "w") as csv_file:
        csv_writer = csv.writer(csv_file)
        csv_writer.writerows(rows)

"""
---------------------------------------CENSUS DATA---------------------------------------
"""

def download_census_data(code, base_dir=''):
  url = f'https://www.nomisweb.co.uk/output/census/2021/census2021-{code.lower()}.zip'
  extract_dir = os.path.join(base_dir, os.path.splitext(os.path.basename(url))[0])

  if os.path.exists(extract_dir) and os.listdir(extract_dir):
    print(f"Files already exist at: {extract_dir}.")
    return

  os.makedirs(extract_dir, exist_ok=True)
  response = requests.get(url)
  response.raise_for_status()

  with zipfile.ZipFile(io.BytesIO(response.content)) as zip_ref:
    zip_ref.extractall(extract_dir)

  print(f"Files extracted to: {extract_dir}")

def load_census_data(code, level='msoa'):
  return pd.read_csv(f'census2021-{code.lower()}/census2021-{code.lower()}-{level}.csv')

def get_student_data(columns_to_drop, column_names):
    student_df = load_census_data("TS062", "oa")
    student_df = student_df.rename(columns={"geography": "OA21CD"})
    student_df = student_df.drop(student_df.columns[columns_to_drop], axis=1).set_index("OA21CD")
    student_df.columns = column_names
    student_df = student_df.div(student_df.sum(axis=1), axis=0)
    return student_df

"""
---------------------------------------OSMNX---------------------------------------
"""

def count_osm_tags(df):
    tag_counter = {}
    for column in df.columns:
        tag_counter[column] = df[column].notnull().sum()
    return tag_counter.items()

def count_pois_near_coordinates(latitude: float, longitude: float, tags: dict, distance_km: float = 1.0):
    box_width = distance_km / 111
    box_height = distance_km / 111
    north = latitude + box_height/2
    south = latitude - box_width/2
    west = longitude - box_width/2
    east = longitude + box_width/2
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        try:
            pois = ox.geometries_from_bbox(north, south, east, west, tags)
        except Exception as e:
            return {}
    df = pd.DataFrame(pois)

    return count_osm_tags(df)

def get_location_to_df(locations_dict, tags, tags_to_keep):
    location_to_df = {}
    for location, (latitude, longitude) in locations_dict.items():
        pois_df = pd.DataFrame(count_pois_near_coordinates(latitude, longitude, tags))
        if pois_df.shape[0] == 0:
            continue
        pois_df.columns = ["tag", "count"]

        pois_df = pois_df[pois_df["tag"].isin(tags_to_keep)]
        location_to_df[location] = pois_df
    return location_to_df

def get_tags_count_with_position(pois_df, lat, long, tags_to_keep):
    all_rows = []
    row_data = {tag:0 for tag in tags_to_keep}
    row_data["LAT"] = lat
    row_data["LONG"] = long
    
    for _, row in pois_df.iterrows():
        row_data[row["tag"]] = row["count"]
    all_rows.append(row_data)
    return pd.DataFrame(all_rows)

def get_all_tags_count(location_to_df, tags_to_keep):
    all_rows = []
    for location, pois_df in location_to_df.items():
        row_data = {tag:0 for tag in tags_to_keep}
        row_data["Location"] = location
        for _, row in pois_df.iterrows():
            row_data[row["tag"]] = row["count"]
        all_rows.append(row_data)
    return pd.DataFrame(all_rows)

def get_bbox(latitude, longitude, bbox_side=1.0):
    box_width = bbox_side / 111
    box_height = bbox_side / 111
    north = latitude + box_height / 2
    south = latitude - box_width / 2
    west = longitude - box_width / 2
    east = longitude + box_width / 2
    return north, south, east, west


def get_osm_tags_near_coordinates(latitude, longitude, tags, bbox_side=1.0):
    north, south, east, west = get_bbox(latitude, longitude, bbox_side)

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        pois = ox.geometries_from_bbox(north, south, east, west, tags)

        # add a default "" if key is not present
        # pois["full_addr"] = (
        #     pois.get("addr:housenumber", "")
        #     + " "
        #     + pois.get("addr:street", "")
        #     + ", "
        #     + pois.get("addr:postcode", "")
        # )
        # pois["addr_full"] = pois.get("addr:housenumber") + " " + pois.get("addr:street") + ", " + pois.get("addr:postcode")
        # pois["has_address"] = pois["addr_full"].str.strip() != ", ,"
        # pois["area_sqm"] = pois.geometry.area
        return pois

"""
---------------------------------------DATABASE---------------------------------------
"""
def download_price_paid_data(year_from, year_to):
    # Base URL where the dataset is stored
    base_url = (
        "http://prod.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com"
    )
    """Download UK house price data for given year range"""
    # File name with placeholders
    file_name = "/pp-<year>-part<part>.csv"
    for year in range(year_from, (year_to + 1)):
        print(f"Downloading data for year: {year}")
        for part in range(1, 3):
            url = base_url + file_name.replace("<year>", str(year)).replace(
                "<part>", str(part)
            )
            response = requests.get(url)
            if response.status_code == 200:
                with open(
                    "."
                    + file_name.replace("<year>", str(year)).replace(
                        "<part>", str(part)
                    ),
                    "wb",
                ) as file:
                    file.write(response.content)

def create_connection(user, password, host, database, port=3306):
    """Create a database connection to the MariaDB database
        specified by the host url and database name.
    :param user: username
    :param password: password
    :param host: host url
    :param database: database name
    :param port: port number
    :return: Connection object or None
    """
    conn = None
    try:
        conn = pymysql.connect(
            user=user,
            passwd=password,
            host=host,
            port=port,
            local_infile=1,
            db=database,
        )
        print(f"Connection established!")
    except Exception as e:
        print(f"Error connecting to the MariaDB Server: {e}")
    return conn

def housing_upload_join_data(conn, year):
    start_date = str(year) + "-01-01"
    end_date = str(year) + "-12-31"

    cur = conn.cursor()
    print("Selecting data for year: " + str(year))
    cur.execute(
        f'SELECT pp.price, pp.date_of_transfer, po.postcode, pp.property_type, pp.new_build_flag, pp.tenure_type, pp.locality, pp.town_city, pp.district, pp.county, po.country, po.latitude, po.longitude, pp.primary_addressable_object_name, pp.secondary_addressable_object_name FROM (SELECT price, date_of_transfer, postcode, property_type, new_build_flag, tenure_type, locality, town_city, district, county, primary_addressable_object_name, secondary_addressable_object_name FROM pp_data WHERE date_of_transfer BETWEEN "'
        + start_date
        + '" AND "'
        + end_date
        + '") AS pp INNER JOIN postcode_data AS po ON pp.postcode = po.postcode'
    )
    rows = cur.fetchall()

    csv_file_path = "output_file.csv"

    # Write the rows to the CSV file
    with open(csv_file_path, "w", newline="") as csvfile:
        csv_writer = csv.writer(csvfile)
        # Write the data rows
        csv_writer.writerows(rows)
    print("Storing data for year: " + str(year))
    cur.execute(
        f"LOAD DATA LOCAL INFILE '"
        + csv_file_path
        + "' INTO TABLE `prices_coordinates_data` FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED by '\"' LINES STARTING BY '' TERMINATED BY '\n';"
    )
    print("Data stored for year: " + str(year))