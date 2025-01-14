from __future__ import annotations

import bz2
from collections import defaultdict, Counter
import dataclasses as dc
import enum
import logging
from typing import (
    Callable, Dict, List, Mapping, Optional, Sequence, Tuple, Union
)

import networkx as nx

@dc.dataclass
class NodeAccouncementData():
    # When we find a path through AS1, we remember the relationship between AS1 and
    # the prior AS in the path. When finding another path through AS1, this then
    # allows us to discard paths that arrive at AS1 from an AS with a less preffered
    # relationship. (HACK: This is an implementation detail and should not be
    # returned to the user.)
    path_pref: dict[int, PathPref] = dc.field(
        default_factory=lambda: defaultdict(lambda: PathPref.UNKNOWN)
    )
    best_paths: dict[int, List[List[int]]] = dc.field(
        default_factory=lambda: defaultdict(list)
    )
    path_len: dict[int, int] = dc.field(default_factory=dict)


NODE_IMPORT_FILTER = "import-filter"
EDGE_REL = "edge-attr-relationship"


class PathPref(enum.IntEnum):
    """Model preference of paths imported by an AS.

    >>> assert PathPref.CUSTOMER > PathPref.PEER
    >>> assert PathPref.PEER > PathPref.PROVIDER
    """

    CUSTOMER = 3
    PEER = 2
    PROVIDER = 1
    UNKNOWN = 0

    @staticmethod
    def from_relationship(graph: ASGraph, exporter: int, importer: int):
        """Compute the PathPref *at importer* given the relationship in the ASGraph."""
        rel = graph.g[importer][exporter][EDGE_REL]
        if rel == Relationship.P2C:
            return PathPref.CUSTOMER
        if rel == Relationship.P2P:
            return PathPref.PEER
        if rel == Relationship.C2P:
            return PathPref.PROVIDER
        raise ValueError(f"Unsupported relationship {rel}")


class Relationship(enum.IntEnum):
    """Model the peering relationship between a pair of ASes.

    Use less-than comparisons to mean better-than:
    >>> assert Relationship.P2C < Relationship.P2P
    >>> assert Relationship.P2P < Relationship.C2P
    """

    C2P = 1
    P2P = 0
    P2C = -1

    def reversed(self):
        """Get Relationship in the opposite direction of an edge.

        >>> assert Relationship.P2C == Relationship.C2P.reversed()
        >>> assert Relationship.P2P == Relationship.P2P.reversed()
        """
        return Relationship(-1 * self.value)


class InferenceCallback(enum.Enum):
    """Callback hooks available in the inference algorithm

    START_RELATIONSHIP_PHASE is called whenever the algorithm starts
    processing relationships of a given type (class Relationship).
    Relationships are processed in order of preference (P2C->P2P->C2P).

    callback(pref: Relationship) -> ()

    VISIT_EDGE is called whenever the algorithm processes an edge. Some
    edges are ignored in the inference process (e.g., edges where the AS
    importing routes is announcing the prefix).

    callback(exporter: int, importer: int, pref: Relationship) -> ()

    NEIGHBOR_ANNOUNCE is called whenever we start a new phase and
    initialize routes at neighbors of origins. Edges from origins to
    neighbors are not considered by VISIT_EDGE.

    callback(origin: int, neighbor: int, pref: Relationship, path: List[int]) -> ()
    """

    START_RELATIONSHIP_PHASE = "start-relationship-phase"
    NEIGHBOR_ANNOUNCE = "neighbor-announce"
    VISIT_EDGE = "visit-edge"


@dc.dataclass
class Announcement:
    """Specification of a prefix announcement.

    A prefix can be announced simulataneously by a set of source ASes.
    Each source AS can announce the prefix to all or a subset of its
    neighbors. Towards each neighbor, a source can manipulate the
    AS-path on its announcement, e.g., to perform AS-path prepending or
    AS-path poisoning.
    """

    source2neighbor2path: Mapping[int, Mapping[int, List[int]]]

    @staticmethod
    def make_anycast_announcement(asgraph: ASGraph, sources: Sequence[int]):
        """Make announcement from sources to all neighbors without prepending.

        Example:

          3  7
          |  |
        4-1  5-6-8
          |
          2

        make_anycast_announcement(graph, [1, 5])

        {
          1: { 2: [], 3: [], 4: [] },
          5: { 7: [], 6: [] },
        }
        """
        src2nei2path: dict[int, dict[int, List[int]]] = dict()
        for src in sources:
            src2nei2path[src] = {nei: [] for nei in asgraph.g[src]}
        return Announcement(src2nei2path)


class WorkQueue:
    def __init__(self):

        # Stores information about which (types of) links are available in the paths
        # to the announcement sources at different path lengths/depths.
        #
        # Example:
        # {
        #     PathPref.CUSTOMER: {
        #         3: [
        #             # a p2c link at depth 3 between AS1001 and AS1005
        #             (1001, 1005),
        #             (1007, 1004)
        #         ],
        #         5: [ (1001, 1004) ],
        #     },
        #     # ...
        # }
        self.pref2depth2edge: Dict[
            PathPref,
            Dict[int, List[Tuple[int, int]]]
        ] = {
            PathPref.CUSTOMER: defaultdict(list),
            PathPref.PEER: defaultdict(list),
            PathPref.PROVIDER: defaultdict(list),
        }

    def get(self, pref: PathPref) -> Union[None, Tuple[int, int]]:
        """Get the edge exporting the shortest paths with pref."""
        if not self.pref2depth2edge[pref]:
            return None
        depth = min(self.pref2depth2edge[pref])
        edge = self.pref2depth2edge[pref][depth].pop()
        if not self.pref2depth2edge[pref][depth]:
            del self.pref2depth2edge[pref][depth]
        return edge

    def add_work(self, graph: ASGraph, node_ann: NodeAccouncementData, exporter: int) -> None:
        """Add work to forward paths at importer to downstream ASes"""
        pref = node_ann.path_pref[exporter]
        for downstream in graph.g[exporter]:
            downstream_pref = PathPref.from_relationship(graph, exporter, downstream)
            if pref == PathPref.CUSTOMER or downstream_pref == PathPref.PROVIDER:
                depth = node_ann.path_len[exporter]
                edge = (exporter, downstream)
                self.pref2depth2edge[downstream_pref][depth].append(edge)

    def check_work(self, graph: ASGraph, node_ann: NodeAccouncementData, exporter: int) -> bool:
        """Check all neighbors importing from exporter are in work queue"""
        pref = node_ann.path_pref[exporter]
        for downstream in graph.g[exporter]:
            downstream_pref = PathPref.from_relationship(graph, exporter, downstream)
            if pref == PathPref.CUSTOMER or downstream_pref == PathPref.PROVIDER:
                depth = node_ann.path_len[exporter]
                edge = (exporter, downstream)
                assert edge in self.pref2depth2edge[downstream_pref][depth]
        return True


class ASGraph:
    def __init__(self):
        self.g = nx.DiGraph()
        self.workqueue = WorkQueue()
        self.announce: Optional[Announcement] = None
        self.callbacks = dict()

    def add_peering(self, source: int, sink: int, relationship: Relationship) -> None:
        """Add nodes and edges corresponding to a peering relationship.

        Note that this adds the relationship bidirectionally. So if you call
        `add_peering(1, 2, Relationship.C2P)` there is no need to call
        `add_peering(2, 1, Relationship.P2C)`.
        """
        if source not in self.g:
            self.g.add_node(source)
            self.g.nodes[source][NODE_IMPORT_FILTER] = None
        if sink not in self.g:
            self.g.add_node(sink)
            self.g.nodes[sink][NODE_IMPORT_FILTER] = None
        self.g.add_edge(source, sink)
        self.g[source][sink][EDGE_REL] = Relationship(relationship)
        self.g.add_edge(sink, source)
        self.g[sink][source][EDGE_REL] = relationship.reversed()

    def set_import_filter(self, asn: int, func: Callable, data=None) -> None:
        """Set import filter for an AS.

        The filter function receives the exporter ASN and the exported
        AS-paths tied for best. The exported AS-paths already include
        the exporter's ASN. It should return the set of AS-paths that
        are actually imported (not discarded). The data variable will be
        passed to the filter function.

        filter(exporter: int, paths: List[Tuple[int]], data) ->
                List[Tuple[int]]
        """
        self.g.nodes[asn][NODE_IMPORT_FILTER] = (func, data)

    def set_callback(self, when: InferenceCallback, func: Callable) -> None:
        """Add a user defined callback for specific points in the execution. By
        default, there are no callbacks set."""
        self.callbacks[when] = func

    def check_announcement(self, announce: Announcement) -> None:
        """Check all relationships exist and that there are no bogus poisonings."""
        for source, neighbor2path in announce.source2neighbor2path.items():
            if source not in self.g:
                raise ValueError(f"Source AS{source} not in ASGraph")
            for neigh, path in neighbor2path.items():
                if neigh not in self.g[source]:
                    raise ValueError(f"Peering AS{source}-AS{neigh} not in ASGraph")
                if neigh in path:
                    raise ValueError(f"Neighbor AS{neigh} poisoned in announcement")

    def infer_paths(
            self, announce: Announcement,
            stop_at_target_asn: Optional[int] = None,
            stop_at_target_count: int = 2,
            initial_node_announcement_data: Optional[NodeAccouncementData] = None,
    ) -> NodeAccouncementData:
        """Infer all AS-paths tied for best toward announcement sources.

        This function performs a modified breadth-first search traversing peering links
        in decreasing order of relationship preference. An AS that has learned a path
        with preference X will never choose paths with preference worse than X nor
        longer paths with preference equal to X. These two properties, combined, allow
        us to compute the best paths directly, without ever generating less preferred or
        longer paths that would eventually be replaced by the best paths.

        A path that traverses a P2P or a C2P link can only be learned through providers.
        After we have processed all routes learnable from (indirect) customers (and only
        customers), there is no need to ever revisit customer routes. ASes choosing
        between multiple provider routes only care about AS-path length: They do not
        care about whether the provider routes traverse a P2P or any number of C2P
        links.

        When `stop_at_target_asn` is given, the simulation of the announcement will
        terminate once `stop_at_target_count` routes have been found. This is useful
        if you only want to specifically find a route between two ASes.

        The `initial_node_announcement_data` parameter is mostly intended for testing
        purposes where it is desirable to have the ability to manually program in some
        existing paths before simulating an announcement.

        The method will RETURN a NodeAnnouncementData object which can be queried for
        data about the simulated announcement, e.g. the best paths to the source ASes
        in the announcements.
        """

        self.check_announcement(announce)
        self.announce = announce
        node_ann = NodeAccouncementData()

        for pref in [PathPref.CUSTOMER, PathPref.PEER, PathPref.PROVIDER]:
            if InferenceCallback.START_RELATIONSHIP_PHASE in self.callbacks:
                self.callbacks[InferenceCallback.START_RELATIONSHIP_PHASE](pref)
            self.make_announcements(pref, node_ann)
            edge = self.workqueue.get(pref)
            while edge:
                if stop_at_target_asn is not None and \
                   len(node_ann.best_paths[stop_at_target_asn]) \
                   > stop_at_target_count:
                    break

                exporter, importer = edge
                if InferenceCallback.VISIT_EDGE in self.callbacks:
                    self.callbacks[InferenceCallback.VISIT_EDGE](
                        exporter, importer, pref
                    )
                if importer in announce.source2neighbor2path:
                    # Do not import route at sources.
                    edge = self.workqueue.get(pref)
                    continue
                assert PathPref.from_relationship(self, exporter, importer) == pref
                if self.update_paths(node_ann, exporter, importer):
                    # only runs when importer has not been processed yet
                    self.workqueue.add_work(self, node_ann, importer)
                edge = self.workqueue.get(pref)

        return node_ann

    def make_announcements(self, pref: PathPref, node_ann: NodeAccouncementData) -> None:
        """Initialize paths with given pref at neighbors according to announcement."""

        # We sort the calls to update_paths() by path length as update_paths() does not
        # allow paths to get shorter due to the breadth-first search.
        nei2len2srcs: Mapping[int, Mapping[int, list[int]]] = defaultdict(lambda: defaultdict(list))
        for src, nei2aspath in self.announce.source2neighbor2path.items():
            for nei, aspath in nei2aspath.items():
                if PathPref.from_relationship(self, src, nei) != pref:
                    continue
                if InferenceCallback.NEIGHBOR_ANNOUNCE in self.callbacks:
                    announce_path = self.announce.source2neighbor2path[src][nei]
                    self.callbacks[InferenceCallback.NEIGHBOR_ANNOUNCE](
                        src, nei, pref, announce_path
                    )
                nei2len2srcs[nei][len(aspath)].append(src)

        for nei, len2srcs in nei2len2srcs.items():
            # We discard all paths longer than the shortest.
            length = min(len2srcs.keys())
            for src in len2srcs[length]:
                announce_path = self.announce.source2neighbor2path[src][nei]
                # Everything in this method before this point was just to discard
                # paths in the announcement longer than the shortest. This is the
                # actual work/side effect:
                if self.update_paths(node_ann, src, nei, announce_path):
                    self.workqueue.add_work(self, node_ann, nei)

    def update_paths(
            self, node_ann: NodeAccouncementData, exporter: int, importer: int,
            announce_path: List[int] = None,
    ) -> bool:
        """Check for new paths or add paths tied for best at importer.

        Returns True if importer just got its first paths (work needs to be enqueued).
        Returns False otherwise, including if importer just learned new paths (in this
        case we check that work is already enqueued).

        The announce_path parameter ignores paths at exporter and allows setting
        arbitrary paths at importer. This is used to handle different announcements to
        different neighbors.
        """
        node = self.g.nodes[importer]
        new_pref = PathPref.from_relationship(self, exporter, importer)
        current_pref = node_ann.path_pref[importer]

        assert current_pref >= new_pref or current_pref == PathPref.UNKNOWN

        # if we already know a path including exporter->importer, we discard any path
        # that would have a lower preference link between exporter->importer
        if current_pref > new_pref: return False

        new_paths = None
        if announce_path is not None:
            assert importer not in announce_path
            new_paths = [[exporter] + announce_path]
        else:
            exported_paths = node_ann.best_paths[exporter]
            # discard routes with cycles
            new_paths = [[exporter] + p for p in exported_paths if importer not in p]

        if node[NODE_IMPORT_FILTER] is not None:
            func, data = node[NODE_IMPORT_FILTER]
            new_paths = func(exporter, new_paths, data)
        if not new_paths:
            return False

        new_path_len = len(new_paths[0])

        if current_pref == PathPref.UNKNOWN:
            node_ann.best_paths[importer] = new_paths
            node_ann.path_len[importer] = new_path_len
            node_ann.path_pref[importer] = new_pref
            return True

        current_path_len = node_ann.path_len[importer]
        assert current_pref == new_pref
        assert new_path_len >= current_path_len
        assert [
            path_is_valley_free(self, p) for p in new_paths
        ].count(False) == 0

        if new_path_len == current_path_len:
            node_ann.best_paths[importer].extend(new_paths)
            assert self.workqueue.check_work(self, node_ann, importer)

        return False


    @staticmethod
    def read_caida_asrel_graph(filepath):
        def parse_relationship_line(line):
            # <provider-as>|<customer-as>|-1
            # <peer-as>|<peer-as>|0
            source, sink, rel = line.strip().split("|")
            return int(source), int(sink), Relationship(int(rel))

        graph = ASGraph()
        cnt = Counter(lines=0, peerings=0)
        with bz2.open(filepath, "rt") as fd:
            for line in fd:
                cnt["lines"] += 1
                if line[0] == "#":
                    # TODO: store metadata in ASGraph
                    continue
                source, sink, rel = parse_relationship_line(line)
                graph.add_peering(source, sink, rel)
                cnt["peerings"] += 1
        logging.info(
            "read %s: %d lines, %d peering relationships",
            filepath,
            cnt["lines"],
            cnt["peerings"],
        )
        return graph


def path_is_valley_free(graph: ASGraph, path: Sequence[int]) -> bool:
    """Check that the given path is valley free.

    Explanation from "AS relationships, customer cones, and validation", Luckie et
    al., 10.1145/2504730.2504735:

    "...each path consists of an uphill segment of zero or more c2p or sibling links,
    zero or one p2p links at the top of the path, followed by a downhill segment of
    zero or more p2c or sibling links"
    """

    relationships: list[Relationship] = [
        graph.g[path[i]][path[i+1]][EDGE_REL] for i in range(0, len(path) - 1)
    ]

    for i in range(0, len(relationships) - 1):
        if relationships[i] < relationships[i+1]: return False

    if relationships.count(Relationship.P2P) > 1: return False

    return True

