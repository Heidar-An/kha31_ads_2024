from .config import *
from . import access, address

import random
import numpy as np
import matplotlib.pyplot as plt
from sklearn.cluster import KMeans
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
from sklearn.neighbors import BallTree

def k_means(data_np, k=3, iterations=75, tolerance=1e-4):
    # used to have consistent values
    random.seed(10)
    num_points, _ = data_np.shape
    # randomly initialize k centroids
    centroids = data_np[random.sample(range(num_points), k)]

    for _ in range(iterations):
        # assign each point to its nearest centroid (calculate distance from each one)
        distances = np.linalg.norm(data_np[:, np.newaxis] - centroids, axis=2)
        cluster_groups = np.argmin(distances, axis=1)

        # compute the new centroid by taking the mean of each cluster
        new_centroids = np.array([
            data_np[cluster_groups == cluster].mean(axis=0) if np.any(cluster_groups == cluster) else centroids[cluster]
            for cluster in range(k)
        ])

        # stop early if there hasn't been a big change between the centroids
        if np.linalg.norm(new_centroids - centroids) < tolerance:
            break
        centroids = new_centroids

    return cluster_groups, centroids

def plot_model_coefficients(rf_model):
    coefficients = rf_model.feature_importances_
    plt.figure(figsize=(10,5))
    plt.bar(access.updated_feature_cols, coefficients)
    plt.xticks(rotation=45, ha='right')
    plt.title("Random Forest Feature Importances")
    plt.ylabel("Coefficient Value")
    plt.tight_layout()
    plt.show()

def get_coordinates_and_ball_tree(osm_merged_df):
    coordinates = osm_merged_df[['LAT', 'LONG']].values
    tree = BallTree(np.radians(coordinates), leaf_size=2)
    return tree

def predict(lat, lon, rf_model, osm_merged_df, tree):
    # find nearest point
    query_point = np.array([[lat, lon]])
    _, ind = tree.query(np.radians(query_point), k=1)
    nearest_idx = ind[0][0]

    nearest_features = osm_merged_df.iloc[nearest_idx][access.updated_feature_cols].values.reshape(1, -1)
    pred_student_pop = rf_model.predict(nearest_features)[0]
    return pred_student_pop