from pyspark import SparkConf, SparkContext
from itertools import combinations


def line_to_list(line):
    return set(sorted(line.split()))


def get_combinations(session, k):
    return [set(sorted(pair)) for pair in combinations(session, k)]


def session_to_groups(session, occurrences_keys):
    result = []
    combs = get_combinations(session, 2) + get_combinations(session, 3)
    for comb in combs:
        if comb.issubset(occurrences_keys):
            result.append(tuple(sorted(comb)))
    return result


conf = SparkConf()
sc = SparkContext(conf=conf)
lines = sc.textFile('4.txt')
data = lines.map(line_to_list)
occurrences = data.flatMap(lambda group: group).countByValue()
occurrences = {k: v for k, v in occurrences.items() if v >= 100}
occurrences_keys = set(occurrences)
print(len(occurrences))
groups = data.flatMap(lambda session: session_to_groups(session, occurrences_keys)).countByValue()

rules = {}
for group, value in groups.items():
    if len(group) == 2:
        occurrences[group] = value
        X, Y = group
        rules[(X, Y)] = value / occurrences[X]
        rules[(Y, X)] = value / occurrences[Y]
    else:
        X, Y, Z = group
        rules[(X, Y, Z)] = value / occurrences[(X, Y)]
        rules[(X, Z, Y)] = value / occurrences[(X, Z)]
        rules[(Y, Z, X)] = value / occurrences[(Y, Z)]

rules = {k: v for k, v in sorted(rules.items(), key=lambda item: (-item[1], item[0]))}
with open('output.csv', 'w') as file:
    for rule, confidence in rules.items():
        file.write(f'{rule} {confidence}\n')


