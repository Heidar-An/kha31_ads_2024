from .config import *

import pandas as pd
import osmnx as ox
import pymysql
import requests
import csv
import warnings
import zipfile
import io

from tqdm import tqdm

feature_cols = [
    "LAT", "LONG", "amenity_count", "bicycle_rental_count",
    "bicycle_parking_count", "capacity_count", "cuisine_count",
    "takeaway_count", "building_count", "brand_count", "TOTAL_RAW_POP"
]

locations_dict = {
    "Nottingham": (52.938, -1.198),
    "Cambridge": (52.2054, 0.1132),
    "Elmbridge": (51.364, -0.394),
    "South Cambridgeshire District": (52.075, 0.1747),
    "Isles of Scilly": (49.913, -6.322),
    "Croydon": (51.3753, -0.0957),
    "Oxford": (51.7570, -1.2545),
    "Euston Square": (51.5246, -0.1340),
    "Temple": (51.5115, -0.1160),
    "Imperial": (51.498, -0.174),
}

# amenity -> education -> everything
# amenity -> sustenance -> [fast_food, cafe]
# amenity -> transportion -> [bicycle_parking, bicycle_rental, bicycle_repair_station]
# amenity -> university
# building -> [dormitory, college, school]

tags = {
        "amenity": [
            "fast_food",
            "cafe",
            "bicycle_parking",
            "bicycle_rental",
            "bicycle_repair_station",
            "university"
        ],
        "building": ["dormitory", "college", "school"],
        "diet": False,
        "addr": False,
        "payment": False,
        "brand": False,
}

# pois = Points of Interest (POIs)
tags_to_keep = ["amenity", "bicycle_rental",
                "bicycle_parking", "capacity",
                "cuisine", "takeaway",
                "building", "brand"]


"""
---------------------------------------INITIALIZE SQL DATABASES---------------------------------------
"""


def initialize_db(conn, name_of_database):
    curr = conn.cursor()
    curr.execute("SET SQL_MODE = 'NO_AUTO_VALUE_ON_ZERO';")
    curr.execute("SET time_zone = '+00:00';")

    curr.execute(f"USE {name_of_database};")

    conn.commit()
    # return conn?


def load_csv_data_into_db(conn, csv_file_name, table_name):
    curr = conn.cursor()

    curr.execute(
        f"""
        LOAD DATA LOCAL INFILE "./{csv_file_name}"
        INTO TABLE `{table_name}`
        FIELDS TERMINATED BY ','
        OPTIONALLY ENCLOSED BY '"'
        LINES TERMINATED BY '\n'
        IGNORE 1 LINES;
    """
    )  # need to ignore first line(s) because it seems to include column names for some reason??
    conn.commit()


def initialize_census_coordinates_db(conn):
    curr = conn.cursor()

    curr.execute("""DROP TABLE IF EXISTS `census_coordinates`;""")
    curr.execute(
        """
        CREATE TABLE IF NOT EXISTS `census_coordinates` (
        `FID` int NOT NULL,
        `OA21CD` VARCHAR(10) COLLATE utf8_bin NOT NULL,
        `LSOA21NM` VARCHAR(255) COLLATE utf8_bin NOT NULL,
        `LSOA21NMW` VARCHAR(255) COLLATE utf8_bin,
        `LAT` decimal(12,5) NOT NULL,
        `LONG` decimal(12,5) NOT NULL,
        PRIMARY KEY (`FID`)
        ) DEFAULT CHARSET=utf8 COLLATE=utf8_bin AUTO_INCREMENT=1;
    """.replace(
            "\n", " "
        )
    )
    curr.execute("""CREATE INDEX geography_code ON `census_coordinates` (`OA21CD`);""")
    conn.commit()

    load_csv_data_into_db(conn, "census_data.csv", "census_coordinates")



def initialize_census_student_pop_db(conn):
    curr = conn.cursor()

    curr.execute("""DROP TABLE IF EXISTS `census_student_pop`;""")
    curr.execute(
        """
        CREATE TABLE IF NOT EXISTS `census_student_pop` (
        `OA21CD` VARCHAR(10) COLLATE utf8_bin NOT NULL,
        `TOTAL_POP` DECIMAL(3, 2) DEFAULT NULL,
        `STUDENT_POP` DECIMAL(3, 2) DEFAULT NULL,
        `TOTAL_RAW_POP` INT DEFAULT NULL,
        PRIMARY KEY (`OA21CD`)
        ) DEFAULT CHARSET=utf8 COLLATE=utf8_bin AUTO_INCREMENT=1;
    """.replace(
            "\n", " "
        )
    )

    curr.execute("""CREATE INDEX geography_code ON `census_student_pop` (`OA21CD`);""")
    conn.commit()

    load_csv_data_into_db(conn, "student_data.csv", "census_student_pop")



def initialize_census_student_coordinates_join_db(conn):
    curr = conn.cursor()

    curr.execute("""DROP TABLE IF EXISTS `census_student_coordinates_join`;""")
    curr.execute(
        """
        CREATE TABLE IF NOT EXISTS `census_student_coordinates_join` (
        `FID` int NOT NULL,
        `OA21CD` VARCHAR(255) COLLATE utf8_bin NOT NULL,
        `LSOA21NM` VARCHAR(255) COLLATE utf8_bin NOT NULL,
        `LSOA21NMW` VARCHAR(255) COLLATE utf8_bin,
        `LAT` decimal(12,5) NOT NULL,
        `LONG` decimal(12,5) NOT NULL,
        `TOTAL_POP` decimal(3,2) DEFAULT NULL,
        `STUDENT_POP` decimal(3,2) DEFAULT NULL,
        `TOTAL_RAW_POP` INT DEFAULT NULL,
        PRIMARY KEY (`FID`)
        ) DEFAULT CHARSET=utf8 COLLATE=utf8_bin AUTO_INCREMENT=1;
    """.replace(
            "\n", " "
        )
    )

    curr.execute(
        """CREATE INDEX geography_code ON `census_student_coordinates_join` (`OA21CD`);"""
    )

    conn.commit()

    load_csv_data_into_db(conn, "census_student_coordinates_join.csv", "census_student_coordinates_join")


def initialize_osm_data_db(conn):
    curr = conn.cursor()

    curr.execute("""DROP TABLE IF EXISTS `osm_data`;""")
    curr.execute(
        """
        CREATE TABLE IF NOT EXISTS `osm_data` (
        `FID` int NOT NULL,
        `LAT` decimal(12,5) NOT NULL,
        `LONG` decimal(12,5) NOT NULL,
        `amenity_count` int DEFAULT 0,
        `bicycle_rental_count` int DEFAULT 0,
        `bicycle_parking_count` int DEFAULT 0,
        `capacity_count` int DEFAULT 0,
        `cuisine_count` int DEFAULT 0,
        `takeaway_count` int DEFAULT 0,
        `building_count` int DEFAULT 0,
        `brand_count` int DEFAULT 0,
        PRIMARY KEY (`FID`)
        ) DEFAULT CHARSET=utf8 COLLATE=utf8_bin AUTO_INCREMENT=1;
    """.replace(
            "\n", " "
        )
    )

    curr.execute("""CREATE INDEX lat_long ON `osm_data` (`LAT`, `LONG`);""")

    conn.commit()

    load_csv_data_into_db(conn, "osm_data.csv", "osm_data")

"""
---------------------------------------READ FROM SQL---------------------------------------
"""

def read_all_data(conn, table_name):
    curr = conn.cursor()
    curr.execute(f"SELECT * FROM {table_name};")
    return curr.fetchall()

def calculate_number_of_rows(conn, table_name):
    curr = conn.cursor()
    curr.execute(f"SELECT COUNT(*) FROM {table_name};")
    return curr.fetchall()

def get_first_row(conn, table_name):
    curr = conn.cursor()
    curr.execute(f"SELECT * FROM {table_name} LIMIT 1;")
    return curr.fetchall()

def get_null_counts(conn, table_name):
    # https://stackoverflow.com/questions/7831371/is-there-a-way-to-get-a-list-of-column-names-in-sqlite
    # using PRAGMA TO GET COLUMN NAMES
    curr = conn.cursor()

    curr.execute(f"PRAGMA table_info({table_name});")
    columns_info = curr.fetchall()
    column_names = [info[1] for info in columns_info]

    null_counts = {}
    
    for column in column_names:
        curr.execute(f"SELECT COUNT(*) FROM {table_name} WHERE {column} IS NULL;")
        null_count = curr.fetchone()[0]
        null_counts[column] = null_count

    curr.execute(f"SELECT COUNT(*) FROM {table_name};")
    total_row_count = curr.fetchone()[0]

    null_counts_df = pd.DataFrame(null_counts.items(), columns=["column_name", "null_counts"])
    null_counts_df = null_counts_df.transpose()

    summary_row = pd.DataFrame([["total_element_count", total_row_count]], columns=null_counts_df.columns)
    return pd.concat([null_counts_df, summary_row])

"""
---------------------------------------CREATE CSVs---------------------------------------
"""

def create_census_student_pop():
    download_census_data("TS062")
    student_df = get_student_data([0,2,4,5,6,7,8,9,10,11], ["TOTAL_POP", "STUDENT_POP"])
    student_df.to_csv("./student_data.csv")


def create_student_coordinates_join(conn):
    census_df = pd.DataFrame(read_all_data(conn, "census_coordinates"))
    student_df = pd.DataFrame(read_all_data(conn, "census_student_pop"))
    merged = census_df.merge(student_df, on="OA21CD")
    merged.to_csv("./census_student_coordinates_join.csv", index=False)

def create_osm_data(conn):
    merged_census_df = pd.DataFrame(read_all_data(conn, "census_student_coordinates_join"))

    osm_tag_counts = []
    # merged_census_df = merged_census_df.sort_values(by=['LAT', 'LONG'])
    # block_size = 100
    # selected_indices = []
    # for i in range(0, len(merged_census_df), block_size):
    #     block = merged_census_df.iloc[i:i+block_size]
    #     random_index = block.sample(n=1).index[0]
    #     selected_indices.append(random_index)
    # selected_rows_df = merged_census_df.loc[selected_indices]

    for _, row in tqdm(merged_census_df.iterrows()):
        latitude = row["LAT"]
        longitude = row["LONG"]
        fid = row["FID"]
        osm_tag_count = count_pois_near_coordinates(latitude, longitude, tags)
        # handle case explicitly when we don't receive any POIs
        if len(osm_tag_count) == 0:
            osm_tag_counts.append(get_all_tags_count_with_position_and_fid(pd.DataFrame([]), fid, latitude, longitude, tags_to_keep))
            continue

        osm_tag_count = pd.DataFrame(osm_tag_count)
        osm_tag_count.columns = ["tag", "count"]

        osm_tag_counts.append(get_all_tags_count_with_position_and_fid(osm_tag_count, fid, latitude, longitude, tags_to_keep))

    osm_counts_df = pd.concat(osm_tag_counts, ignore_index=True)
    osm_counts_df.to_csv("./osm_data.csv", index=False)


"""
---------------------------------------CENSUS DATA---------------------------------------
"""

def download_census_data(code, base_dir=""):
    url = f"https://www.nomisweb.co.uk/output/census/2021/census2021-{code.lower()}.zip"
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


def load_census_data(code, level="msoa"):
    return pd.read_csv(
        f"census2021-{code.lower()}/census2021-{code.lower()}-{level}.csv"
    )


def get_student_data(columns_to_drop, column_names):
    student_df = load_census_data("TS062", "oa")
    student_df = student_df.rename(columns={"geography": "OA21CD"})
    student_df = student_df.drop(student_df.columns[columns_to_drop], axis=1).set_index(
        "OA21CD"
    )
    student_df.columns = column_names
    student_raw_total_pop = student_df["TOTAL_POP"]
    student_df = student_df.div(student_df.sum(axis=1), axis=0)
    student_df["TOTAL_RAW_POP"] = student_raw_total_pop
    return student_df


"""
---------------------------------------OSMNX---------------------------------------
"""


def count_osm_tags(df):
    tag_counter = {}
    for column in df.columns:
        tag_counter[column] = df[column].notnull().sum()
    return tag_counter.items()

def get_all_tags_count_with_position_and_fid(pois_df, fid, lat, long, tags_to_keep):
    all_rows = []
    row_data = {tag:0 for tag in tags_to_keep}
    row_data["FID"] = fid
    row_data["LAT"] = lat
    row_data["LONG"] = long

    if not pois_df.empty:
        pois_df = pois_df[pois_df["tag"].isin(tags_to_keep)]
    for _, row in pois_df.iterrows():
        row_data[row["tag"]] = row["count"]
    all_rows.append(row_data)
    return pd.DataFrame(all_rows)

def count_pois_near_coordinates(
    latitude: float, longitude: float, tags: dict, distance_km: float = 1.0
):
    box_width = distance_km / 111
    box_height = distance_km / 111
    north = latitude + box_height / 2
    south = latitude - box_width / 2
    west = longitude - box_width / 2
    east = longitude + box_width / 2
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
    row_data = {tag: 0 for tag in tags_to_keep}
    row_data["LAT"] = lat
    row_data["LONG"] = long

    for _, row in pois_df.iterrows():
        row_data[row["tag"]] = row["count"]
    all_rows.append(row_data)
    return pd.DataFrame(all_rows)


def get_all_tags_count(location_to_df, tags_to_keep):
    all_rows = []
    for location, pois_df in location_to_df.items():
        row_data = {tag: 0 for tag in tags_to_keep}
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


"""
---------------------------------------OTHER---------------------------------------
"""

# https://stackoverflow.com/questions/76804871/create-save-and-load-spatial-index-using-geopandas
# Found mention of the geopandas and shapely library from the link above (after trying to learn how spatial indices work
# after another student mentioned it to me)

# census_gdf = gpd.GeoDataFrame(
#     merged_df,
#     geometry=gpd.points_from_xy(merged_df['LONG'], merged_df['LAT']),
#     crs="EPSG:4326"
# )

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
Project 2:
------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------
"""

"""
---------------------------------------INITIALIZE SQL DATABASES---------------------------------------
"""


def initialize_general_health_db(conn):
    curr = conn.cursor()

    curr.execute("""DROP TABLE IF EXISTS `general_health`;""")
    curr.execute(
        """
        CREATE TABLE IF NOT EXISTS `general_health` (
        `db_id` bigint(20) unsigned NOT NULL,
        `local_authorities_code` VARCHAR(10) NOT NULL,
        `local_authorities` VARCHAR(255) NOT NULL,
        `general_health_code` int NOT NULL,
        `general_health` VARCHAR(255) NOT NULL,
        `observation` INT NOT NULL,
        PRIMARY KEY (`db_id`)
        ) DEFAULT CHARSET=utf8 COLLATE=utf8_bin AUTO_INCREMENT=1;
    """.replace(
            "\n", " "
        )
    )

    curr.execute("""ALTER TABLE `general_health` MODIFY `db_id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,AUTO_INCREMENT=1;""")

    curr.execute("""CREATE INDEX local_authorities_code ON `general_health` (`local_authorities_code`);""")
    conn.commit()

    load_csv_data_into_db(conn, "general_health.csv", "census_coordinates")



def initialize_education_db(conn):
    curr = conn.cursor()

    curr.execute("""DROP TABLE IF EXISTS `education`;""")
    curr.execute(
        """
        CREATE TABLE IF NOT EXISTS `education` (
        `db_id` bigint(20) unsigned NOT NULL,
        `local_authorities_code` VARCHAR(10) NOT NULL,
        `local_authorities` VARCHAR(255) NOT NULL,
        `level_of_education_code` int NOT NULL,
        `level_of_education` VARCHAR(512) NOT NULL,
        `observation` INT NOT NULL,
        PRIMARY KEY (`db_id`)
        ) DEFAULT CHARSET=utf8 COLLATE=utf8_bin AUTO_INCREMENT=1;
    """.replace(
            "\n", " "
        )
    )
    curr.execute("""ALTER TABLE `education` MODIFY `db_id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,AUTO_INCREMENT=1;""")

    curr.execute("""CREATE INDEX local_authorities_code ON `education` (`local_authorities_code`);""")
    conn.commit()

    load_csv_data_into_db(conn, "level_of_education.csv", "education")


def initialize_income_db(conn):
    curr = conn.cursor()

    curr.execute("""DROP TABLE IF EXISTS `income`;""")
    curr.execute(
        """
        CREATE TABLE IF NOT EXISTS `income` (
        `db_id` bigint(20) unsigned NOT NULL,
        `local_authorities_code` VARCHAR(10) NOT NULL,
        `region` VARCHAR(255) NOT NULL,
        `local_authority` VARCHAR(255) NOT NULL,
        `tenth_percentile` int NOT NULL,
        `fiftieth_percentile` int NOT NULL,
        `ninetieth_percentile` int NOT NULL,
        PRIMARY KEY (`db_id`)
        ) DEFAULT CHARSET=utf8 COLLATE=utf8_bin AUTO_INCREMENT=1;
    """.replace(
            "\n", " "
        )
    )

    curr.execute("""ALTER TABLE `income` MODIFY `db_id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,AUTO_INCREMENT=1;""")

    curr.execute("""CREATE INDEX local_authorities_code ON `income` (`local_authorities_code`);""")
    conn.commit()

    load_csv_data_into_db(conn, "income_statistics_removed.csv", "income")
