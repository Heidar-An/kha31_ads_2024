from .config import *

from . import access

"""These are the types of import we might expect in this file
import pandas
import bokeh
import seaborn
import sklearn.decomposition as decomposition
import sklearn.feature_extraction"""

"""Place commands in this file to assess the data you have downloaded. How are missing values encoded, how are outliers encoded? What do columns represent, makes rure they are correctly labeled. How is the data indexed.
Crete visualisation routines to assess the data (e.g. in bokeh). Ensure that date formats are correct and correctly timezoned."""
import matplotlib.pyplot as plt
import osmnx as ox


def data():
    """Load the data from access and ensure missing values are correctly encoded as well as indices correct, column names informative, date and times correctly formatted. Return a structured data structure such as a data frame."""
    df = access.data()
    raise NotImplementedError

def query(data):
    """Request user input for some aspect of the data."""
    raise NotImplementedError

def view(data):
    """Provide a view of the data that allows the user to verify some aspect of its quality."""
    raise NotImplementedError

def labelled(data):
    """Provide a labelled set of data ready for supervised learning."""
    raise NotImplementedError

def plot_buildings(pois, latitude, longitude, bbox_side):
    north, south, east, west = access.get_bbox(latitude, longitude, bbox_side)

    graph  = ox.graph_from_bbox(north, south, east, west)
    nodes, edges = ox.graph_to_gdfs(graph)

    buildings_with_address = pois[pois["has_address"]]
    buildings_without_address = pois[~pois["has_address"]]

    fig, ax = plt.subplots(figsize=(12, 10))

    edges.plot(ax=ax, color="dimgray", linewidth=1)
    buildings_with_address.plot(ax=ax, color="blue", label="With Address")
    buildings_without_address.plot(ax=ax, color="red", label="Without Address")

    # getting issues if I use a []
    ax.set_xlim(west, east)
    ax.set_ylim(south, north)
    ax.set_xlabel("Longitude")
    ax.set_ylabel("Latitude")

    plt.title("Buildings in the Area")
    plt.legend()
    plt.tight_layout()
    plt.show()
