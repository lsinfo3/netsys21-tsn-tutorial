from numpy import cumsum
from queue import Queue
from typing import Dict, List, Union


class Node(object):
    def __init__(self, type: str, name: str) -> None:
        if "-" in name: raise ValueError("Node name may not contain '-'")
        valid_types = ("switch", "host")
        if type not in valid_types: raise ValueError("Node type must be one of %s, not %s" % (valid_types, type))
        self.type: str = type
        self.name: str = name
        self.neighs: Dict[Node, Link] = {}
        self.last_port = 0

    def add_neighbor(self, l: object) -> None:
        if not isinstance(l, Link): raise ValueError("l has do be an instance of Link, not %s" % l.__class__)
        if self.name not in (n.name for n in l.nodes):
            raise ValueError("l (%s) must contain this node (%s)" % (l.name, self.name))
        n = l.get_other(self)
        if n not in self.neighs:
            self.neighs[n] = l
            n.add_neighbor(l)

    def set_and_get_next_port(self):
        self.last_port += 1
        return self.last_port

    def __eq__(self, o: object) -> bool:
        return isinstance(o, self.__class__) and (self.type, self.name) == (o.type, o.name)

    def __ne__(self, o: object) -> bool:
        return not self.__eq__(o)

    def __hash__(self) -> int:
        return self.name.__hash__()

    def __str__(self) -> str:
        return self.name

    def __repr__(self) -> str:
        return self.__str__()


class Switch(Node):
    def __init__(self, name: str) -> None:
        super().__init__("switch", name)


class Host(Node):
    def __init__(self, name: str) -> None:
        super().__init__("host", name)


class Link(object):
    def __init__(self, n1: Node, n2: Node, bandwidth: float) -> None:
        """
        :param bandwidth: in Bit/s
        """
        if n1.name == n2.name:
            raise ValueError("n1 and n2 refer to the same Node %s" % n1.name)
        self.nodes = sorted([n1, n2], key=lambda n: n.name)
        self.ports = [self.nodes[0].set_and_get_next_port(), self.nodes[1].set_and_get_next_port()]
        self.name = "%s-%s" % (self.nodes[0].name, self.nodes[1].name)
        self.bandwidth = bandwidth
        """
        bandwidth in Bit/s
        """

    def get_other(self, n: Union[str, Node]) -> Node:
        if isinstance(n, Node):
            n = n.name
        if self.nodes[0].name == n:
            return self.nodes[1]
        return self.nodes[0]

    def get_port_of_node(self, n: Union[str, Node]):
        if isinstance(n, Node):
            n = n.name
        if self.nodes[0].name == n:
            return self.ports[0]
        if self.nodes[1].name == n:
            return self.ports[1]
        raise ValueError("Node %s is not part of this link (%s)" % (n, self))

    def __eq__(self, o: object) -> bool:
        return isinstance(o, self.__class__) and (self.name) == (o.name)

    def __ne__(self, o: object) -> bool:
        return not self.__eq__(o)

    def __hash__(self) -> int:
        return self.name.__hash__()

    def __str__(self) -> str:
        return self.name

    def __repr__(self) -> str:
        return self.__str__()


class Topology(object):
    def __init__(self, per_hop_guarantees: Dict[str, Dict[int, int]] = None) -> None:
        self.nodes = {}
        self.links = {}
        self.streams = {}
        self.streams_per_link = {}
        self.per_hop_guarantees = per_hop_guarantees
        """
        The per_hop_guarantees are defined per link and per priority.
        
        per_hop_guarantees := {linkname -> {priority -> delay}}
        
        Do not adjust this variable directly. Use update_guarantees() instead.
        """

    def add_stream(self, stream) -> None:
        self.streams[stream.stream_id] = stream

        for linkname in stream.get_directed_link_list():
            if linkname not in self.streams_per_link:
                self.streams_per_link[linkname] = {}
            self.streams_per_link[linkname][stream.stream_id] = stream

        if self.per_hop_guarantees != None:
            self.update_acc_latencies(stream)

    def remove_stream(self, stream) -> None:
        del self.streams[stream.stream_id]
        for linkname in stream.get_directed_link_list():
            del self.streams_per_link[linkname][stream.stream_id]

    def remove_all_streams(self) -> None:
        self.streams = {}
        self.streams_per_link = {}

    def update_acc_latencies(self, stream) -> None:
        linknamelist = stream.get_directed_link_list()
        linkobjectlist = [self.get_link_by_name(link) for link in linknamelist]

        accMaxLatencies = cumsum([stream.maxFrameSize / (linkobjectlist[0].bandwidth / 1e9)] +
                                 [self.per_hop_guarantees[linkname][stream.priority] for linkname in linknamelist])
        accMinLatencies = cumsum([stream.minFrameSize / (link.bandwidth / 1e9) for link in linkobjectlist])

        stream.accMaxLatencies = {}
        stream.accMinLatencies = {}
        for i, linkname in enumerate(linknamelist):
            stream.accMaxLatencies[linkname] = accMaxLatencies[i]
            stream.accMinLatencies[linkname] = accMinLatencies[i]

    def update_guarantees(self, per_hop_guarantees: Dict[str, Dict[int, float]]) -> None:
        self.per_hop_guarantees = per_hop_guarantees
        for stream in self.streams.values():
            self.update_acc_latencies(stream)

    def update_guarantees_all_links(self, per_hop_guarantees: Dict[int, float]) -> None:
        full_guarantees = {}
        for linkname in self.get_all_links_directed():
            full_guarantees[linkname] = per_hop_guarantees
        self.update_guarantees(full_guarantees)

    def get_all_links_directed(self):
        ret = []
        for link in self.links.values():
            ret.append("%s-%s" % (link.nodes[0].name, link.nodes[1].name))
            ret.append("%s-%s" % (link.nodes[1].name, link.nodes[0].name))
        return ret

    def add_node(self, n: Node) -> Node:
        self.nodes[n.name] = n
        return self.nodes[n.name]

    def add_link(self, l: Link) -> Link:
        self.add_node(l.nodes[0])
        self.add_node(l.nodes[1])
        self.links[l.name] = l
        l.nodes[0].add_neighbor(l)
        l.nodes[1].add_neighbor(l)
        return self.links[l.name]

    def get_node(self, name: str) -> Node:
        if isinstance(name, Node): name = name.name
        return self.nodes.get(name)

    def create_and_add_link(self, n1: Union[str, Node], n2: Union[str, Node], bandwidth: float) -> Link:
        """
        :param bandwidth: in Bit/s
        """
        if isinstance(n1, Node): n1 = n1.name
        if isinstance(n2, Node): n2 = n2.name
        node1 = self.get_node(n1)
        node2 = self.get_node(n2)
        link = Link(node1, node2, bandwidth)
        self.add_link(link)
        return link

    def create_and_add_neigh(self, n1: Union[str, Node], node2: Node, bandwidth: float) -> Node:
        """
        :param bandwidth: in Bit/s
        """
        if isinstance(n1, Node): n1 = n1.name
        node1 = self.get_node(n1)
        self.add_node(node2)
        self.create_and_add_link(node1.name, node2.name, bandwidth)
        return node2

    def get_link(self, n1: Union[str, Node], n2: Union[str, Node]) -> Link:
        if isinstance(n1, Node): n1 = n1.name
        if isinstance(n2, Node): n2 = n2.name
        nodes_sorted = sorted([n1, n2])
        return self.links.get(nodes_sorted[0] + "-" + nodes_sorted[1])

    def get_link_by_name(self, linkname: str) -> Link:
        return self.get_link(linkname.split("-")[0], linkname.split("-")[1])

    def shortest_path(self, name1: Union[str, Node], name2: Union[str, Node]) -> List[Node]:
        if isinstance(name1, Node): name1 = name1.name
        if isinstance(name2, Node): name2 = name2.name

        n1 = self.get_node(name1)
        n2 = self.get_node(name2)

        pi = {}
        pi[n1] = None

        q = Queue()
        q.put(n1)

        while not q.empty():
            node = q.get()

            if node == n2:
                path = []
                while node != None:
                    path.insert(0, node)
                    node = pi[node]
                return path

            for neigh in node.neighs:
                if neigh not in pi:
                    q.put(neigh)
                    pi[neigh] = node

        raise ValueError("No path from %s to %s exists" % (n1.name, n2.name))

