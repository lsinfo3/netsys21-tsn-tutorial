import math
from typing import Dict, Set, Any, List

from lib.stream import Stream
from lib.topology import Topology

PREAMBLE = 8 * 8  # Bit
IPG = 12 * 8  # Bit
MAX_BE_FRAME = 1522 * 8 + PREAMBLE + IPG


def latency_bound_ats(topology: Topology, link: str, priority: int, max_be_frame: int = MAX_BE_FRAME) -> int:
    sum_rates = 0
    sum_bursts = 0
    max_lower_prio_frame = max_be_frame
    min_equal_prio_frame = max_be_frame

    # Return 0 if no stream with that priority is found on that link
    if link not in topology.streams_per_link or priority not in [s.priority for s in topology.streams_per_link[link].values()]:
        return 0
    for stream in topology.streams_per_link[link].values():
        if stream.priority > priority:
            sum_bursts += stream.burst
            sum_rates += stream.rate
        elif stream.priority == priority:
            sum_bursts += stream.burst
            min_equal_prio_frame = min(min_equal_prio_frame, stream.minFrameSize)
        else:
            max_lower_prio_frame = max(max_lower_prio_frame, stream.maxFrameSize)

    sum_bursts = sum_bursts - min_equal_prio_frame + max_lower_prio_frame
    link_speed = topology.get_link_by_name(link).bandwidth / 1e9
    remaining_speed = link_speed - sum_rates / 1e9

    return math.ceil(sum_bursts / remaining_speed + min_equal_prio_frame / link_speed)


def max_number_of_bursts_sp(topology: Topology, link: str, priority: int, stream: Stream) -> int:
    inter_burst_interval = stream.burst / (stream.rate / 1e9)
    current_hop_min_latency = stream.minFrameSize / (topology.get_link_by_name(link).bandwidth / 1e9)  # + processing, + propagation delay

    if priority == stream.priority:
        time_frame = stream.accMaxLatencies[link] - (stream.accMinLatencies[link] - current_hop_min_latency)
        return math.ceil(time_frame / inter_burst_interval)
    elif priority < stream.priority:
        time_frame = stream.accMaxLatencies[link] - (stream.accMinLatencies[link] - current_hop_min_latency) + topology.per_hop_guarantees[link][priority]
        return math.ceil(time_frame / inter_burst_interval)
    else:
        return 0


def latency_bound_sp(topology: Topology, link: str, priority: int, max_be_frame: int = MAX_BE_FRAME) -> int:
    sum_bursts = 0
    max_lower_prio_frame = max_be_frame

    # Return 0 if no stream with that priority is found on that link
    if link not in topology.streams_per_link or priority not in [s.priority for s in topology.streams_per_link[link].values()]:
        return 0
    for stream in topology.streams_per_link[link].values():
        sum_bursts += max_number_of_bursts_sp(topology, link, priority, stream) * stream.burst
        if stream.priority < priority:
            max_lower_prio_frame = max(max_lower_prio_frame, stream.maxFrameSize)

    sum_bursts += max_lower_prio_frame
    return math.ceil(sum_bursts / (topology.get_link_by_name(link).bandwidth / 1e9))