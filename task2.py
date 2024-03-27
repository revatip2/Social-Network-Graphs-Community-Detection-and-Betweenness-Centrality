from pyspark import SparkContext
import sys
import time
import os
from itertools import combinations
from collections import defaultdict
from pyspark.sql import *


filter_threshold = int(sys.argv[1])
input_file_path = sys.argv[2]
between_output_file_path = sys.argv[3]
comm_output_file_path = sys.argv[4]

spark = SparkSession.builder.appName("graphs_task2").getOrCreate()

sc = SparkContext.getOrCreate()

start = time.time()

yelp_data_rdd = sc.textFile(input_file_path)
row_one = yelp_data_rdd.first()
yelp_data_rdd = yelp_data_rdd.filter(lambda a: a != row_one).map(lambda a: a.split(','))

biz_rdd = yelp_data_rdd.map(lambda row: (row[0], {row[1]}))
uz_biz_rdd = biz_rdd.reduceByKey(lambda a, b: a.union(b))
uz_biz_data = dict(uz_biz_rdd.collect())
user_ids = uz_biz_rdd.keys().collect()
user_pairs = list(combinations(user_ids, 2))
user_pairs_rdd = sc.parallelize(user_pairs)

edges = []
for u in user_pairs:
    (uz1, uz2) = u
    biz1 = uz_biz_data[uz1]
    biz2 = uz_biz_data[uz2]
    common_businesses = biz1.intersection(biz2)
    if len(common_businesses) >= filter_threshold:
        edges.append((uz1, uz2))
        edges.append((uz2, uz1))

unique_user_ids = set()
for tuple_item in edges:
    unique_user_ids.add(tuple_item[0])
    unique_user_ids.add(tuple_item[1])

vertices = list(unique_user_ids)

# Betweenness
graph = {}
for u1, u2 in edges:
    graph.setdefault(u1, set()).add(u2)
    graph.setdefault(u2, set()).add(u1)

def initialize_graph():
    return defaultdict(set), {}, defaultdict(float), [], [], set()

def calculate_betweenness(route, root_node, num_short):
    credit_node = {node_val: 1 for node_val in route}
    credit_edge = defaultdict(float)
    between_list = defaultdict(float)

    for node_val in reversed(route):
        for val in root_node[node_val]:
            assigned = credit_node[node_val] * (num_short[val] / num_short[node_val])
            credit_node[val] += assigned

            edge_key = tuple(sorted([node_val, val]))
            credit_edge[edge_key] += assigned

    for k,v in credit_edge.items():
        between_list[k] += v / 2

    output_bwness = sorted(between_list.items(), key = lambda x: (-x[1], x[0]))

    return output_bwness

def girvann_new(graph, vertices):
    between_list = defaultdict(float)

    def calc_bw_update(node):
        nonlocal between_list
        root_node, depth, num_short, route, q, traversed = initialize_graph()
        traversed.add(node)
        q.append(node)
        depth[node] = 0
        num_short[node] = 1

        while q:
            r = q.pop(0)
            route.append(r)

            for neighbour in graph[r]:
                if neighbour not in traversed:
                    q.append(neighbour)
                    traversed.add(neighbour)
                    if neighbour not in root_node.keys():
                        root_node[neighbour] = set()
                        root_node[neighbour].add(r)
                    else:
                        root_node[neighbour].add(r)
                    num_short[neighbour] += num_short[r]
                    depth[neighbour] = depth[r] + 1
                elif depth[neighbour] == depth[r] + 1:
                    root_node[neighbour].add(r)
                    num_short[neighbour] += num_short[r]

        betweenness = calculate_betweenness(route, root_node, num_short)

        for key, value in betweenness:
            between_list[key] += value

    for node in vertices:
        calc_bw_update(node)
    sorted_betweenness = sorted(between_list.items(), key=lambda x: (-x[1], x[0]))

    return sorted_betweenness

#
# graph = {
#         'user1': {'user2', 'user3', 'user4'},
#         'user2': {'user1', 'user3'},
#         'user3': {'user1', 'user2'},
#         'user4': {'user1', 'user5'},
#         'user5': {'user4'}
#     }

# vertices = list(graph.keys())
final_between = girvann_new(graph, vertices)
with open(between_output_file_path, 'w') as file:
    for key, value in final_between:
        formatted_output = f"('{key[0]}', '{key[1]}'),{round(value, 5)}\n"
        file.write(formatted_output)
# Community

def initialize_graph_comm(graph, vertices, bw):
    s = {n: set(neighbors) for n, neighbors in graph.items()}
    num_edges = len(bw)
    k = {n: len(graph[n]) for n in graph}
    return s, num_edges, k

def find_comms(s, vertices):
    candidates = []
    vert = set(vertices.copy())
    while vert:
        root = vert.pop()
        q = [root]
        traversed = {root}
        while q:
            current = q.pop(0)
            for node in s[current]:
                if node not in traversed:
                    vert.remove(node)
                    q.append(node)
                    traversed.add(node)
        travelled = sorted(list(traversed))
        candidates.append(travelled)
    return candidates

def calculate_modularity(graph, k, num_edges, community):
    modularity = 0.0
    for com in community:
        for i in com:
            for j in com:
                a_i_j = 1.0 if j in graph[i] else 0.0
                modularity += a_i_j - (k[i] * k[j]) / (2.0 * num_edges)

    modularity = modularity / (2 * num_edges)
    return modularity

def update_graph(s, bw):
    max_bw = bw[0][1]
    for vertex, value in bw:
        if value >= max_bw:
            s[vertex[0]].remove(vertex[1])
            s[vertex[1]].remove(vertex[0])
    return s

def community_detection(graph, vertices, bw):
    s, num_edge, k = initialize_graph_comm(graph, vertices, bw)
    cur_val = -float('inf')
    comms = []
    while len(bw) > 0:
        communi = find_comms(s, vertices)
        modularity = calculate_modularity(graph, k, num_edge, communi)
        if modularity > cur_val:
            cur_val = modularity
            comms = [list(c) for c in communi]
        s = update_graph(s, bw)
        bw = girvann_new(s, vertices)
    comms = sorted(comms, key=lambda x: (len(x), x[0]))
    return comms

result_communities = community_detection(graph, vertices, final_between)

with open(comm_output_file_path, 'w') as text_file:
    for i in result_communities:
        text_file.write(str(i)[1:-1] + "\n")

#print(result_communities)
end = time.time()

print('Duration: ',end - start)
sc.stop()