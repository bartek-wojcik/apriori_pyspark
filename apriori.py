from pyspark import SparkConf, SparkContext
from itertools import combinations


def line_to_list(line):
    return set(sorted(line.split()))


def get_combinations(session, k):
    return [set(sorted(pair)) for pair in combinations(session, k)]


def session_to_groups(session):
    result = list(session)
    combs = get_combinations(session, 2) + get_combinations(session, 3)
    for comb in combs:
        result.append(tuple(sorted(comb)))
    return result


conf = SparkConf()
sc = SparkContext(conf=conf)
lines = sc.textFile('4.txt')
data = lines.map(line_to_list)
groups = data.flatMap(session_to_groups).countByValue()
occurrences = {k: v for k, v in groups.items() if v >= 100}

rules = {}
for group, value in occurrences.items():
    if len(group) == 2:
        X, Y = group
        rules[(X, Y)] = value / occurrences[X]
        rules[(Y, X)] = value / occurrences[Y]
    elif len(group) == 3:
        X, Y, Z = group
        rules[(X, Y, Z)] = value / occurrences[(X, Y)]
        rules[(X, Z, Y)] = value / occurrences[(X, Z)]
        rules[(Y, Z, X)] = value / occurrences[(Y, Z)]

rules = {k: v for k, v in sorted(rules.items(), key=lambda item: (-item[1], item[0]))}
with open('output.csv', 'w') as file:
    for rule, confidence in rules.items():
        file.write(f'{rule} {confidence}\n')


