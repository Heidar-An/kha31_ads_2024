from .config import *

from . import access, address

import matplotlib.pyplot as plt
import osmnx as ox
import numpy as np
import pandas as pd
from sklearn.linear_model import Ridge, Lasso
from sklearn.model_selection import cross_val_score
from statistics import mean
from sklearn.linear_model import LinearRegression
from sklearn.metrics import r2_score
from scipy.stats import pearsonr
import statsmodels.api as sm


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


cols = ["L1-L3", "L4-L6", "L7", "L8-L9", "L10-11", "L12", "L13", "L14.1-L14.2", "L15"]

def plot_label_model(ages, population, results, label = "UK", model_type = "Linear"):
    plt.figure(figsize=(10,6))
    plt.scatter(ages, population, label = "population")

    plt.plot(ages, results.predict(), 'r-', label = f"{label} {model_type} Model")


    plt.legend()
    plt.xlabel("Age")
    plt.ylabel("Population")
    plt.title("Age Distribution and Model Fits")
    plt.show()

def plot_for_features(norm_age_df, columns_to_drop, column_names):
    access.download_census_data('TS062')
    # print(norm_age_df.shape)
    student_df = access.load_census_data('TS062', "ltla")
    # keep geography + L15 (full time students) + total residents
    student_df = student_df.drop(student_df.columns[columns_to_drop], axis=1).set_index('geography')
    # print(student_df.shape)
    student_df.columns = column_names
    # normalize the data
    student_df = student_df.div(student_df.sum(axis=1), axis=0)

    merged_df = pd.merge(norm_age_df[[21]], student_df, left_index=True, right_index=True)

    # take values (remove index) and reshape so it has one column (otherwise, it has (x, ) shape) and doesn't work :(
    X = merged_df['student_population'].values.reshape(-1, 1)

    # print(X.values.reshape(-1, 1).shape)
    y = merged_df[21].values

    model = LinearRegression()
    model.fit(X, y)

    y_pred = model.predict(X)

    plt.figure(figsize=(8, 6))
    plt.scatter(y, y_pred)
    plt.xlabel('Actual Percentage of 21-year-olds')
    plt.ylabel('Predicted Percentage of 21-year-olds')
    plt.title('Correlation between Actual and Predicted Percentage of 21-year-olds')
    plt.show()

    r2 = r2_score(y, y_pred)
    print(f"R-squared: {r2}")

    corr, _ = pearsonr(y, y_pred)
    print(f"Correlation: {corr}")

    # ALTERNATIVE SOLUTION WITH THE OTHER LIBRARY YOU USE
    m_linear = sm.OLS(y, X)
    results = m_linear.fit()

    y_pred_linear = results.get_prediction(X).summary_frame(alpha=0.05)

    plt.figure(figsize=(8, 6))
    plt.scatter(y, y_pred_linear["mean"])
    plt.xlabel('Actual Percentage of 21-year-olds')
    plt.ylabel('Predicted Percentage of 21-year-olds')
    plt.title('Correlation between Actual and Predicted Percentage of 21-year-olds')
    plt.show()

    r2_alternative = r2_score(y, y_pred_linear["mean"])
    print(f"R-squared: {r2_alternative}")
    print(results.summary())

    return r2, corr

def predict_age_profile(city_nssec, best_model_for_age):
    X_city = np.array([city_nssec[col] for col in cols]).reshape(1, -1)
    predicted_profile = np.zeros(100)
    for age in range(100):
        model = best_model_for_age[age]
        predicted_profile[age] = model.predict(X_city)[0]
    return predicted_profile


def plot_comparison(predicted_profile, actual_profile, city_name):
  plt.figure(figsize=(10, 6))
  plt.plot(range(100), predicted_profile, label="Predicted")
  plt.plot(range(100), actual_profile, label="Actual")
  plt.xlabel("Age")
  plt.ylabel("Proportion")
  plt.title(f"Age Profile Comparison for {city_name}")
  plt.legend()
  plt.show()


def plot_predicted_student(y, y_pred):
    plt.figure(figsize=(8, 6))
    plt.scatter(y, y_pred)
    plt.xlabel('Actual Percentage of Student Population')
    plt.ylabel('Predicted Percentage of Student Population')
    plt.title('Correlation between Actual and Predicted Percentage of Student Population Proportion')
    plt.show()