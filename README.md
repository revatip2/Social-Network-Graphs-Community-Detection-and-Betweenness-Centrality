# Graph Analysis with PySpark: Community Detection and Betweenness Centrality

This repository contains code for analyzing graph data, specifically focusing on community detection and betweenness centrality computation on large scale datasets.

## Table of Contents

- [Introduction](#introduction)
- [Theory](#theory)
  - [Community Detection](#community-detection)
  - [Betweenness Centrality](#betweenness-centrality)
- [Usage](#usage)
  - [Code A](#code-a)
  - [Code B](#code-b)
- [Conclusion](#conclusion)

## Introduction

Graph analysis plays a crucial role in various domains such as social networks, recommendation systems, and biological networks. Community detection and betweenness centrality are fundamental techniques in graph theory used to understand the structure and organization of networks.

## Theory

### Community Detection

Community detection aims to identify cohesive groups of nodes within a graph. The Girvan-Newman algorithm is a popular approach for community detection, which iteratively removes edges with high betweenness centrality to reveal the underlying community structure.

### Betweenness Centrality

Betweenness centrality measures the importance of a node or edge in a graph by quantifying the number of shortest paths that pass through it. In the context of community detection, edges with high betweenness centrality often serve as bridges between communities.

## Usage

### Code A

This code snippet performs community detection using the GraphFrames library in PySpark. It reads an input dataset, constructs a graph based on user interactions with businesses, and applies the label propagation algorithm for community detection.

To use Code A:

1. Ensure PySpark is properly configured.
2. Run the script with the following command-line arguments:

```python script_name.py <filter_threshold> <input_file_path> <output_file_path>```

- `<filter_threshold>`: Integer threshold value for filtering common businesses.
- `<input_file_path>`: Path to the input dataset file.
- `<output_file_path>`: Path to store the output results.

### Code B

This code snippet implements a custom algorithm for community detection and betweenness centrality calculation. It reads an input dataset, constructs a graph, and applies the Girvan-Newman algorithm for community detection.

To use Code B:

1. Ensure PySpark is properly configured.
2. Run the script with the following command-line arguments:

```python script_name.py <filter_threshold> <input_file_path> <between_output_file_path> <comm_output_file_path>```

- `<filter_threshold>`: Integer threshold value for filtering common businesses.
- `<input_file_path>`: Path to the input dataset file.
- `<between_output_file_path>`: Path to store the betweenness centrality results.
- `<comm_output_file_path>`: Path to store the community detection results.

## Conclusion

Community detection and betweenness centrality analysis are essential techniques for understanding the structure of complex networks. By leveraging PySpark and graph analysis libraries, such as GraphFrames, researchers and practitioners can gain valuable insights into the organization and connectivity of large-scale graphs.
