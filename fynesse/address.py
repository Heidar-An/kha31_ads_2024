# This file contains code for suporting addressing questions in the data
from .config import *

"""# Here are some of the imports we might expect
import sklearn.model_selection  as ms
import sklearn.linear_model as lm
import sklearn.svm as svm
import sklearn.naive_bayes as naive_bayes
import sklearn.tree as tree

import GPy
import torch
import tensorflow as tf

# Or if it's a statistical analysis
import scipy.stats"""

"""Address a particular question that arises from the data"""

import random
import numpy as np

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