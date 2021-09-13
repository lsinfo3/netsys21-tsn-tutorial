import pandas as pd
from typing import List
from lib.topology import Node

PREAMBLE = 8 * 8  # Bit
IPG = 12 * 8  # Bit


class Stream(object):
    LAST_ID = -1

    def __init__(self, label: str, path: List[Node], priority: int, rate: float, burst: int, minFrameSize: int, maxFrameSize: int) -> None:
        """
        :param rate: in bits/s
        :param burst: in bit (including overheads PREAMBLE + IPG)
        :minFrameSize: in bit (excluding overhead)
        :maxFrameSize: in bit (excluding overhead)
        """
        Stream.LAST_ID += 1
        self.stream_id = Stream.LAST_ID
        self.label = label
        self.path = path
        self.priority = priority
        self.rate = rate
        self.burst = burst
        self.minFrameSize = minFrameSize
        self.maxFrameSize = maxFrameSize
        self.accMaxLatencies = {}
        self.accMinLatencies = {}

    def get_directed_link_list(self) -> List[str]:
        ret = []
        for i in range(0, len(self.path)-1):
            n1 = self.path[i].name
            n2 = self.path[i+1].name
            ret.append("%s-%s" % (n1, n2))
        return ret

    def get_residence_time_df(self, frame_count: int = 5):
        inter_burst_interval = self.burst / (self.rate / 1e9)
        data = []
        for f in range(frame_count):
            frame_origin = f * inter_burst_interval
            for i, linkname in enumerate(self.get_directed_link_list()):
                data.append([str(f + 1), i + 1, linkname, "accMinLatency", frame_origin + self.accMinLatencies[linkname]])
                data.append([str(f + 1), i + 1, linkname, "accMaxLatency", frame_origin + self.accMaxLatencies[linkname]])
        return pd.DataFrame(data, columns=["frame", "hop", "link", "type", "latency"])

    def clone(self):
        return Stream(self.label, self.path, self.priority, self.rate, self.burst, self.minFrameSize, self.maxFrameSize)