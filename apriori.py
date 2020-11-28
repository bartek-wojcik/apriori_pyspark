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

rules_2 = {}
rules_3 = {}
for group, value in occurrences.items():
    if len(group) == 2:
        X, Y = group
        rules_2[(X, Y)] = value / occurrences[X]
        rules_2[(Y, X)] = value / occurrences[Y]
    elif len(group) == 3:
        X, Y, Z = group
        rules_3[(X, Y, Z)] = value / occurrences[(X, Y)]
        rules_3[(X, Z, Y)] = value / occurrences[(X, Z)]
        rules_3[(Y, Z, X)] = value / occurrences[(Y, Z)]


def sort_rules(rules):
    return {k: v for k, v in sorted(rules.items(), key=lambda item: (-item[1], item[0]))}


def write_rules_to_file(rules, filename):
    with open(filename + '.csv', 'w') as file:
        for rule, confidence in rules.items():
            file.write(f'{rule} {confidence}\n')


rules_2 = sort_rules(rules_2)
rules_3 = sort_rules(rules_3)
write_rules_to_file(rules_2, '2')
write_rules_to_file(rules_3, '3')
