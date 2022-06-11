from calendar import c
from collections import defaultdict
from typing import List
import itertools

from copy import deepcopy


def create_trace_reverse_index(trace, name: str = "concept:name"):
    """
    Create reverse index mapping for trace, e.g. the following. ER registration is on places 0, 1, 2 in the log.
    {
    "ER Registration": [0, 1, 2],
    "Event 2": [3, 4, 5]
    }
    
    @param trace: trace in xml log
    @param name: which attribute will be used to create the reverse index
    """

    rev_index = defaultdict(list)
    for ind, event in enumerate(trace):
        rev_index[event[name]].append(ind)

    return rev_index


def create_log_reverse_index(log, name: str = "concept:name"):
    """
    Run create_trace_reverse_index(.) over the whole log
    
    @param log: log to create a reverse index for
    @param name: whicch attribute will be used to create the reverse log index
    
    [rev_index_trace_1, rev_index_trace_2, ...]
    """

    reverse_indexes = []
    for trace in log:
        reverse_indexes.append(create_trace_reverse_index(trace, name))

    return reverse_indexes


#### Constraint checking and payload checking

## Declarative constraints
def check_response_constraint(rev_trace, a, b):
    """
    Response constraint: Check that every A is followed by B.
    """
    if a not in rev_trace:
        return 1  #  vacuously fulfilled
    if b not in rev_trace:
        return 0  #  can't be fulfilled

    # Check that every A is followed by B.
    # => Check that the last A is followed by B
    last_a_ind = rev_trace[a][-1]
    last_b_ind = rev_trace[b][-1]

    if last_b_ind > last_a_ind:
        return 2  #  fulfilled

    #  cant be fulfilled
    return 0


def check_init_constraint(rev_trace, a):
    """
    Init constraint: check that A is in the first location
    """
    if a not in rev_trace:
        return 0  #  not in trace

    a_ind = rev_trace[a][0]
    if a_ind == 0:
        return 2  #  first location

    return 0  #  not in first location


def check_precedence_constraint(rev_trace, a, b):
    """
    Precedence constraint: check that every B has A before it. 
    Find first B, check if there is an A before it.
    """
    if b not in rev_trace:
        return 1  #  vacuously fulfilled
    if a not in rev_trace:
        return 0  #  can't be fulfilled

    first_a_ind = rev_trace[a][0]
    first_b_ind = rev_trace[b][0]

    # Check that every B has A before it.
    # => Check that the first B is preceeded by A

    if first_a_ind < first_b_ind:
        return 2  #  fulfilled

    #  cant be fulfilled
    return 0


def check_existence_constraint(rev_trace, a):
    """
    Existence constraint: check that A exists in trace
    """
    if a not in rev_trace:
        return 0  #  not in trace

    return 2  # is in trace


#### Sequence labellers
def check_sequence_strict_order(
    trace,
    sequence: List[str],
    count: int = 1,
    hack_delim: str = "␟",
    name: str = "concept:name",
):
    """
    Check if sequence exists in the trace at least count times. Strict sequence.
    Activities have to be consecutive.
    
    Input is normal trace, no reverse index
    """

    if len(trace) < count:
        # There cant be the sequence, as the length of trace is smaller than sequence
        return 0

    search_string = hack_delim.join([event[name] for event in trace])
    search_seq = hack_delim.join(sequence)

    seq_count = search_string.count(search_seq)

    if seq_count >= count:
        return 2  # fulfilled

    return 0  # not fulfilled


def check_sequence_nonstrict_order_interleaved(
    trace, sequence: List[str], count: int = 1
):
    """
    Non-strict order. Consider 
    """

    sequence_activities = set(sequence)

    count_subsequences = 0
    # Nonstrict order. Check how many times we complete sequence
    copy_seq = sequence[:]

    for event in trace:
        name = event["concept:name"]
        # Try to remove
        if name not in sequence_activities:
            continue
        try:
            copy_seq.remove(name)
        except:
            continue

        # If one subsequence is matched, we can continue
        if len(copy_seq) == 0:
            # print(count_subsequences, "Count!")
            count_subsequences += 1
            copy_seq = sequence[:]

    if count_subsequences >= count:
        return 2

    return 0


def check_sequence_strict_order_interleaved(trace, sequence: List[str], count: int = 1):
    """
    Non-strict order. Consider 
    """

    sequence_activities = set(sequence)

    count_subsequences = 0
    # Nonstrict order. Check how many times we complete sequence
    copy_seq = sequence[:]

    for event in trace:
        name = event["concept:name"]
        # Try to remove
        if name not in sequence_activities:
            continue

        if name == copy_seq[0]:
            copy_seq.pop(0)

        # If one subsequence is matched, we can continue
        if len(copy_seq) == 0:
            count_subsequences += 1
            copy_seq = sequence[:]

    if count_subsequences >= count:
        return 2

    return 0


def check_sequence_nonstrict_order(
    trace,
    sequence: List[str],
    count: int = 1,
    hack_delim: str = "␟",
    name: str = "concept:name",
):
    """
    Check if sequence exists in the trace at least count times. Non-strict sequence
    
    Input is normal trace, no reverse index
    """

    if len(trace) < count:
        return 0

    tested_permutations = set()
    # Get all unique orders of inp sequence

    search_list = [event[name] for event in trace]

    found_indices = []
    # Try every unique permutation
    for perm_seq in itertools.permutations(sequence):
        if perm_seq in tested_permutations:
            continue
        tested_permutations.add(perm_seq)

        #  Very non-performant way of implementing string-search stuff.. For longer sequences could use sth similar to Boyer-Moore, KMP, Horspool.
        for i in range(len(search_list) - len(sequence)):
            if tuple(search_list[i : i + len(sequence)]) == perm_seq:
                found_indices.append(i)

    # If not enough found indices, then shorcircuit.
    if len(found_indices) < count:
        return 0

    # Get rid of indice overlaps, keep the earliest to maximize (greedy, should be fine)
    #  Remove duplicates and sort
    found_indices = sorted(list(set(found_indices)))

    # Filter out remaining. Must keep gap of at least len(sequence) - not true, because textual strings are longer.
    filtered_indices = [found_indices[0]]
    next_min_index = found_indices[0] + len(sequence)
    for i in range(1, len(found_indices)):
        next_ind = found_indices[i]
        if next_ind >= next_min_index:
            filtered_indices.append(next_ind)
            next_min_index = next_ind + len(sequence)

    if len(filtered_indices) >= count:
        return 2

    return 0


### Attribute labellers
def check_trace_attribute(trace, attribute_value_dict):
    """
    Checks if trace attribute is equal to the value in attribute value dict.
    """
    trace_attributes = trace.attributes
    matched_count = 0
    for k, v in attribute_value_dict.items():
        if k in trace_attributes and trace_attributes[k] == str(v):
            matched_count += 1
        else:
            return 0  # at least one doesnt match

    if matched_count == len(attribute_value_dict):
        # all matches
        return 2

    return 0


def check_one_trace_attribute(trace, attribute, val):
    """
    Splits all attributes ":" and checks if any prefix fits the label.
    E.g. if there is "Treatment code:1", then it is changed to "Treatment code" before it is checked.
    """
    trace_attributes = trace.attributes
    for attrib, attrib_val in trace_attributes.items():
        key = attrib
        if ":" in key:
            key = attrib.split(":")[0]

        if key == attribute and str(val) == str(attrib_val):
            return 2

    return 0


def check_trace_event_attributes(trace, attribute_value_dict: dict):
    """
    Collect all event attributes which have key in attribute_value_dict
    Keep first one if it is a repetition.
    Check if match attribute value dict
    """
    event_attributes = {}
    for event in trace:
        for k in attribute_value_dict.keys():
            if k in event and k not in event_attributes:
                event_attributes[k] = str(event[k])

    #  All attributes collected, now check if conditions match

    matched_count = 0
    for k, v in attribute_value_dict.items():
        if k in event_attributes and event_attributes[k] == v:
            matched_count += 1
        else:
            return 0  # at least one doesnt match

    if matched_count == len(attribute_value_dict):
        # all matches
        return 2

    return 0


def check_trace_event_match_atleast_once(trace, attribute, val):
    """
    Collect all event attributes which have key in attribute_value_dict
    """
    for event in trace:
        if attribute in event and str(event[attribute]) == str(val):
            return 2

    return 0
