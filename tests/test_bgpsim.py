import itertools
import os
import random
import unittest
import unittest.mock
import urllib.parse
import urllib.request

from bgpsim import (
    Announcement,
    ASGraph,
    NodeAccouncementData,
    PathPref,
    Relationship,
    WorkQueue,
    EDGE_REL,
)


SLOW_TESTS_DISABLED = True
CAIDA_AS_RELATIONSHIPS_URL = (
    "http://data.caida.org/datasets/as-relationships/serial-1/20200101.as-rel.txt.bz2"
)


def _make_graph_implicit_withdrawal():
    # Make AS graph that requires a BGP implicit withdrawal prior to convergence.
    # AS2 is a peer of AS3, but is not a client of AS1.
    # AS8 should learn 8 3 1 10 then change to 8 6 4 1 10 when
    # AS3 finally learns the route through AS2.
    # 1--------\---\
    # |    2---3   4
    # |    5   |   6
    # |    7   8---/
    # |    9
    # 10---/
    graph = ASGraph()
    graph.add_peering(1, 3, Relationship(-1))
    graph.add_peering(1, 4, Relationship(-1))
    graph.add_peering(1, 10, Relationship(-1))
    graph.add_peering(2, 3, Relationship(0))
    graph.add_peering(2, 5, Relationship(-1))
    graph.add_peering(3, 8, Relationship(-1))
    graph.add_peering(4, 6, Relationship(-1))
    graph.add_peering(5, 7, Relationship(-1))
    graph.add_peering(6, 8, Relationship(-1))
    graph.add_peering(7, 9, Relationship(-1))
    graph.add_peering(9, 10, Relationship(-1))
    return graph


def _make_graph_implicit_withdrawal_multihop():
    # Test implicit withdrawal and distant propagation.
    # AS11 will route towards AS10 through AS2 and discard route through AS1.
    # 1---11
    # |   | \
    # |   2  3
    # | / |   \
    # 10  12   4
    graph = ASGraph()
    graph.add_peering(1, 11, Relationship.P2P)
    graph.add_peering(10, 1, Relationship.C2P)
    graph.add_peering(10, 2, Relationship.C2P)
    graph.add_peering(2, 11, Relationship.C2P)
    graph.add_peering(4, 3, Relationship.C2P)
    graph.add_peering(3, 11, Relationship.C2P)
    graph.add_peering(12, 2, Relationship.C2P)
    return graph


def _make_graph_preferred():
    # Test AS3 routes correctly to AS3; multiple routes of different preferences.
    # 2----3-\
    # |    | |
    # \ 1--5 |
    #  -4  | |
    #   6--/-/
    graph = ASGraph()
    graph.add_peering(1, 4, Relationship.P2C)
    graph.add_peering(1, 5, Relationship.P2P)
    graph.add_peering(2, 3, Relationship.P2P)
    graph.add_peering(2, 4, Relationship.P2C)
    graph.add_peering(3, 6, Relationship.P2C)
    graph.add_peering(4, 6, Relationship.P2C)
    graph.add_peering(5, 6, Relationship.P2C)
    return graph


def _make_graph_multiple_choices():
    # Test ASes learn and correctly propagate multiple routes:
    # 1---\---\
    # |   |   |
    # 2   3   4===6   # 2, 3, 4 peer with 6
    # |   |   |   |
    # 5---/---/   7
    # |
    # |---\---\
    # 8   9   10===12   # 8, 9, 10 peer with 12
    # |   |   |    |
    # 11--/---/    13
    graph = ASGraph()
    graph.add_peering(1, 2, Relationship.P2C)
    graph.add_peering(1, 3, Relationship.P2C)
    graph.add_peering(1, 4, Relationship.P2C)
    graph.add_peering(2, 5, Relationship.P2C)
    graph.add_peering(3, 5, Relationship.P2C)
    graph.add_peering(4, 5, Relationship.P2C)
    graph.add_peering(2, 6, Relationship.P2P)
    graph.add_peering(3, 6, Relationship.P2P)
    graph.add_peering(4, 6, Relationship.P2P)
    graph.add_peering(6, 7, Relationship.P2C)
    graph.add_peering(5, 8, Relationship.P2C)
    graph.add_peering(5, 9, Relationship.P2C)
    graph.add_peering(5, 10, Relationship.P2C)
    graph.add_peering(8, 11, Relationship.P2C)
    graph.add_peering(9, 11, Relationship.P2C)
    graph.add_peering(10, 11, Relationship.P2C)
    graph.add_peering(8, 12, Relationship.P2P)
    graph.add_peering(9, 12, Relationship.P2P)
    graph.add_peering(10, 12, Relationship.P2P)
    graph.add_peering(12, 13, Relationship.P2C)
    return graph


def _make_graph_peer_peer_relationships():
    # Test route propagation through sequence of P2P links:
    # AS9 is a provider of 1 and 5. AS10 is a provider of 3 and 7.
    # 9-------\   10
    # |    /--+--/|
    # 1---3---5---7
    # 2   4   6   8
    graph = ASGraph()
    graph.add_peering(1, 2, Relationship.P2C)
    graph.add_peering(3, 4, Relationship.P2C)
    graph.add_peering(5, 6, Relationship.P2C)
    graph.add_peering(7, 8, Relationship.P2C)
    graph.add_peering(9, 1, Relationship.P2C)
    graph.add_peering(9, 5, Relationship.P2C)
    graph.add_peering(10, 3, Relationship.P2C)
    graph.add_peering(10, 7, Relationship.P2C)
    graph.add_peering(1, 3, Relationship.P2P)
    graph.add_peering(3, 5, Relationship.P2P)
    graph.add_peering(5, 7, Relationship.P2P)
    return graph


def _check_origin(_exporter, paths, origin):
    return [p for p in paths if p[-1] == origin]


def _make_graph_peer_lock():
    # Test propagation of hijacked routes through when using peer lock:
    # ASes 2 and 3 peer with AS1, ASes 4 and 5 are providers of AS1.
    # ASes 2 and 4 have peer lock configured with AS1. ASes 6 and 7 are
    # customers of ASes 2-5. AS7 will hijack the prefix. AS 8 peers with
    # ASes 2-5 and AS9 is a provider of ASes 2-5.
    #   ----9----
    #  /   / \   \
    # |   4   5   |  --\
    # |  | \ / |  |  --\
    # 2--+--1--+--3----8
    # |  |     |  |  --/
    #  \-6     7-/
    graph = ASGraph()
    graph.add_peering(1, 2, Relationship.P2P)
    graph.add_peering(1, 3, Relationship.P2P)
    graph.add_peering(1, 4, Relationship.C2P)
    graph.add_peering(1, 5, Relationship.C2P)
    graph.add_peering(6, 2, Relationship.C2P)
    graph.add_peering(6, 3, Relationship.C2P)
    graph.add_peering(6, 4, Relationship.C2P)
    graph.add_peering(6, 5, Relationship.C2P)
    graph.add_peering(7, 2, Relationship.C2P)
    graph.add_peering(7, 3, Relationship.C2P)
    graph.add_peering(7, 4, Relationship.C2P)
    graph.add_peering(7, 5, Relationship.C2P)
    graph.add_peering(8, 2, Relationship.P2P)
    graph.add_peering(8, 3, Relationship.P2P)
    graph.add_peering(8, 4, Relationship.P2P)
    graph.add_peering(8, 5, Relationship.P2P)
    graph.add_peering(9, 2, Relationship.P2C)
    graph.add_peering(9, 3, Relationship.P2C)
    graph.add_peering(9, 4, Relationship.P2C)
    graph.add_peering(9, 5, Relationship.P2C)
    graph.set_import_filter(2, _check_origin, 1)
    graph.set_import_filter(4, _check_origin, 1)
    return graph


class TestPathPref(unittest.TestCase):
    def test_comparison(self):
        self.assertTrue(PathPref.CUSTOMER > PathPref.PEER)
        self.assertTrue(PathPref.PEER > PathPref.PROVIDER)
        self.assertTrue(PathPref.PROVIDER > PathPref.UNKNOWN)

    def test_from_relationship(self):
        graph = _make_graph_implicit_withdrawal()
        for src, snk, relationship in graph.g.edges.data(EDGE_REL): # type: ignore
            if relationship == Relationship.P2C:
                pref = PathPref.from_relationship(graph, src, snk)
                self.assertEqual(pref, PathPref.PROVIDER)
                pref = PathPref.from_relationship(graph, snk, src)
                self.assertEqual(pref, PathPref.CUSTOMER)
            elif relationship == Relationship.P2P:
                pref = PathPref.from_relationship(graph, src, snk)
                self.assertEqual(pref, PathPref.PEER)
                pref = PathPref.from_relationship(graph, snk, src)
                self.assertEqual(pref, PathPref.PEER)


class TestRelationship(unittest.TestCase):
    def test_comparison(self):
        p2p = Relationship(Relationship.P2P.value)
        c2p = Relationship(Relationship.C2P.value)
        p2c = Relationship(Relationship.P2C.value)
        self.assertEqual(p2p, Relationship.P2P)
        self.assertEqual(c2p, Relationship.C2P)
        self.assertEqual(p2c, Relationship.P2C)

    def test_sort(self):
        self.assertTrue(Relationship.P2C < Relationship.P2P)
        self.assertTrue(Relationship.P2P < Relationship.C2P)

    def test_reversed(self):
        self.assertEqual(Relationship.P2P, Relationship.P2P.reversed())
        self.assertEqual(Relationship.C2P, Relationship.P2C.reversed())
        self.assertEqual(Relationship.P2C, Relationship.C2P.reversed())


class TestAnnouncement(unittest.TestCase):
    def test_make_anycast_announcemenet(self):
        def test_sources(sources):
            announce = Announcement.make_anycast_announcement(graph, sources)
            self.assertEqual(set(sources), set(announce.source2neighbor2path.keys()))
            for source in sources:
                neighbor2path = announce.source2neighbor2path[source]
                self.assertEqual(set(graph.g[source]), set(neighbor2path.keys()))
                for aspath in neighbor2path.values():
                    self.assertEqual(aspath, [])

        graph = _make_graph_implicit_withdrawal()
        test_sets = [[1, 10], [2, 3], [7, 6, 2], [1, 2, 7, 9, 8]]
        for sources in test_sets:
            test_sources(sources)


class TestWorkQueue(unittest.TestCase):
    def setUp(self):
        # Preconfigure paths for 3 and 7, with longer AS-paths at 7:
        self.graph = _make_graph_implicit_withdrawal()
        self.node_ann = NodeAccouncementData()
        self.node_ann.best_paths[3] = [[]]
        self.node_ann.path_len[3] = 0
        self.node_ann.path_pref[3] = PathPref.CUSTOMER
        self.node_ann.best_paths[7] = [[ 7, 7 ]]
        self.node_ann.path_len[7] = 2
        self.node_ann.path_pref[7] = PathPref.CUSTOMER
        self.workqueue = WorkQueue()
        self.workqueue.add_work(self.graph, self.node_ann, 3)
        self.workqueue.add_work(self.graph, self.node_ann, 7)

    def test_add_work(self):
        pref2depth2edge = self.workqueue.pref2depth2edge
        self.assertEqual(len(pref2depth2edge), 3)
        self.assertEqual(len(pref2depth2edge[PathPref.CUSTOMER]), 2)
        self.assertCountEqual(list(pref2depth2edge[PathPref.CUSTOMER]), [0, 2])
        self.assertEqual(len(pref2depth2edge[PathPref.PROVIDER]), 2)
        self.assertCountEqual(list(pref2depth2edge[PathPref.PROVIDER]), [0, 2])
        self.assertEqual(len(pref2depth2edge[PathPref.PEER]), 1)
        self.assertCountEqual(list(pref2depth2edge[PathPref.PEER]), [0])

    def test_get(self):
        self.assertEqual(self.workqueue.get(PathPref.CUSTOMER), (3, 1))
        self.assertEqual(self.workqueue.get(PathPref.CUSTOMER), (7, 5))
        self.assertIsNone(self.workqueue.get(PathPref.CUSTOMER))
        self.assertEqual(self.workqueue.get(PathPref.PEER), (3, 2))
        self.assertIsNone(self.workqueue.get(PathPref.PEER))
        self.assertEqual(self.workqueue.get(PathPref.PROVIDER), (3, 8))
        self.assertEqual(self.workqueue.get(PathPref.PROVIDER), (7, 9))
        self.assertIsNone(self.workqueue.get(PathPref.PROVIDER))


class TestASGraph(unittest.TestCase):
    def test_implicit_withdraw(self):
        graph = _make_graph_implicit_withdrawal()
        g1 = graph.clone()

        announce = Announcement.make_anycast_announcement(graph, [10])
        node_ann = graph.infer_paths(announce)
        self.assertListEqual(node_ann.best_paths[8], [[6, 4, 1, 10]])
        self.assertEqual(node_ann.path_pref[8], PathPref.PROVIDER)
        self.assertListEqual(node_ann.best_paths[3], [[2, 5, 7, 9, 10]])
        self.assertEqual(node_ann.path_pref[3], PathPref.PEER)
        self.assertListEqual(node_ann.best_paths[1], [[10,]])
        self.assertEqual(node_ann.path_pref[1], PathPref.CUSTOMER)

        announce = Announcement.make_anycast_announcement(g1, [4])
        node_ann = g1.infer_paths(announce)
        self.assertListEqual(node_ann.best_paths[8], [[6, 4]])
        self.assertEqual(node_ann.path_pref[8], PathPref.PROVIDER)
        self.assertListEqual(node_ann.best_paths[3], [[1, 4]])
        self.assertEqual(node_ann.path_pref[3], PathPref.PROVIDER)
        self.assertListEqual(node_ann.best_paths[10], [[1, 4]])
        self.assertEqual(node_ann.path_pref[10], PathPref.PROVIDER)
        self.assertEqual(node_ann.path_pref[2], PathPref.UNKNOWN)
        self.assertEqual(node_ann.path_pref[5], PathPref.UNKNOWN)
        self.assertEqual(node_ann.path_pref[7], PathPref.UNKNOWN)
        self.assertEqual(node_ann.path_pref[9], PathPref.UNKNOWN)

    def test_implicit_withdrawal_multihop(self):
        graph = _make_graph_implicit_withdrawal_multihop()
        g1 = graph.clone()

        announce = Announcement.make_anycast_announcement(graph, [10])
        node_ann = graph.infer_paths(announce)
        self.assertListEqual(node_ann.best_paths[11], [[2, 10]])
        self.assertEqual(node_ann.path_pref[11], PathPref.CUSTOMER)
        self.assertListEqual(node_ann.best_paths[4], [[3, 11, 2, 10]])
        self.assertEqual(node_ann.path_pref[4], PathPref.PROVIDER)
        self.assertListEqual(node_ann.best_paths[12], [[2, 10]])
        self.assertEqual(node_ann.path_pref[12], PathPref.PROVIDER)
        self.assertListEqual(node_ann.best_paths[1], [[10,]])
        self.assertEqual(node_ann.path_pref[1], PathPref.CUSTOMER)

        announce = Announcement.make_anycast_announcement(g1, [2])
        node_ann = g1.infer_paths(announce)
        self.assertListEqual(node_ann.best_paths[11], [[2,]])
        self.assertEqual(node_ann.path_pref[11], PathPref.CUSTOMER)
        self.assertListEqual(node_ann.best_paths[4], [[3, 11, 2]])
        self.assertEqual(node_ann.path_pref[4], PathPref.PROVIDER)
        self.assertListEqual(node_ann.best_paths[12], [[2,]])
        self.assertEqual(node_ann.path_pref[12], PathPref.PROVIDER)
        self.assertListEqual(node_ann.best_paths[1], [[11, 2]])
        self.assertEqual(node_ann.path_pref[1], PathPref.PEER)

    def test_preferred(self):
        graph = _make_graph_preferred()
        announce = Announcement.make_anycast_announcement(graph, [4])
        node_ann = graph.infer_paths(announce)
        self.assertListEqual(node_ann.best_paths[3], [[2, 4]])
        self.assertEqual(node_ann.path_pref[3], PathPref.PEER)
        self.assertListEqual(node_ann.best_paths[5], [[1, 4]])
        self.assertEqual(node_ann.path_pref[5], PathPref.PEER)
        self.assertListEqual(node_ann.best_paths[6], [[4,]])
        self.assertEqual(node_ann.path_pref[6], PathPref.PROVIDER)

    def test_multiple_choices_from_provider(self):
        graph = _make_graph_multiple_choices()
        announce = Announcement.make_anycast_announcement(graph, [1])
        node_ann = graph.infer_paths(announce)

        self.assertEqual(node_ann.path_pref[6], PathPref.UNKNOWN)
        self.assertEqual(node_ann.path_pref[7], PathPref.UNKNOWN)
        self.assertEqual(node_ann.path_pref[12], PathPref.UNKNOWN)
        self.assertEqual(node_ann.path_pref[13], PathPref.UNKNOWN)

        as5_paths = [[2, 1], [3, 1], [4, 1]]
        self.assertCountEqual(node_ann.best_paths[5], as5_paths)
        self.assertEqual(node_ann.path_pref[5], PathPref.PROVIDER)

        as8_paths = [[5, 2, 1], [5, 3, 1], [5, 4, 1]]
        self.assertCountEqual(node_ann.best_paths[8], as8_paths)
        self.assertEqual(node_ann.path_pref[8], PathPref.PROVIDER)

        as11_paths = [
            [8, 5, 2, 1],
            [8, 5, 3, 1],
            [8, 5, 4, 1],
            [9, 5, 2, 1],
            [9, 5, 3, 1],
            [9, 5, 4, 1],
            [10, 5, 2, 1],
            [10, 5, 3, 1],
            [10, 5, 4, 1],
        ]
        self.assertCountEqual(node_ann.best_paths[11], as11_paths)

    def test_multiple_choices_from_customer(self):
        graph = _make_graph_multiple_choices()
        announce = Announcement.make_anycast_announcement(graph, [11])
        node_ann = graph.infer_paths(announce)

        as13_paths = [[12, 10, 11], [12, 9, 11], [12, 8, 11]]
        self.assertCountEqual(node_ann.best_paths[13], as13_paths)
        self.assertEqual(node_ann.path_pref[13], PathPref.PROVIDER)

        as7_paths = [
            [6, 2, 5, 10, 11],
            [6, 2, 5, 9, 11],
            [6, 2, 5, 8, 11],
            [6, 3, 5, 10, 11],
            [6, 3, 5, 9, 11],
            [6, 3, 5, 8, 11],
            [6, 4, 5, 10, 11],
            [6, 4, 5, 9, 11],
            [6, 4, 5, 8, 11],
        ]
        self.assertCountEqual(node_ann.best_paths[7], as7_paths)
        self.assertEqual(node_ann.path_pref[7], PathPref.PROVIDER)

        as1_paths = [
            [2, 5, 10, 11],
            [2, 5, 9, 11],
            [2, 5, 8, 11],
            [3, 5, 10, 11],
            [3, 5, 9, 11],
            [3, 5, 8, 11],
            [4, 5, 10, 11],
            [4, 5, 9, 11],
            [4, 5, 8, 11],
        ]
        self.assertCountEqual(node_ann.best_paths[1], as1_paths)
        self.assertEqual(node_ann.path_pref[1], PathPref.CUSTOMER)

    def test_multiple_provider_sources(self):
        graph = _make_graph_multiple_choices()
        announce = Announcement.make_anycast_announcement(graph, [2, 4])
        node_ann = graph.infer_paths(announce)

        as1_paths = [[2,], [4,]]
        self.assertCountEqual(node_ann.best_paths[1], as1_paths)
        self.assertEqual(node_ann.path_pref[1], PathPref.CUSTOMER)

        as3_paths = [[1, 4], [1, 2]]
        self.assertCountEqual(node_ann.best_paths[3], as3_paths)
        self.assertEqual(node_ann.path_pref[3], PathPref.PROVIDER)

        as7_paths = [[6, 4], [6, 2]]
        self.assertCountEqual(node_ann.best_paths[7], as7_paths)
        self.assertEqual(node_ann.path_pref[7], PathPref.PROVIDER)

        as11_paths = [
            [8, 5, 4],
            [8, 5, 2],
            [9, 5, 4],
            [9, 5, 2],
            [10, 5, 4],
            [10, 5, 2],
        ]
        self.assertCountEqual(node_ann.best_paths[11], as11_paths)
        self.assertEqual(node_ann.path_pref[11], PathPref.PROVIDER)

        self.assertEqual(node_ann.path_pref[12], PathPref.UNKNOWN)
        self.assertEqual(node_ann.path_pref[13], PathPref.UNKNOWN)

    def test_multiple_provider_sources_prepend(self):
        graph = _make_graph_multiple_choices()
        announce = Announcement.make_anycast_announcement(graph, [2, 4])
        announce.source2neighbor2path[2][5] = (2,) # type: ignore
        node_ann = graph.infer_paths(announce)

        as1_paths = [[2,], [4,]]
        self.assertCountEqual(node_ann.best_paths[1], as1_paths)
        self.assertEqual(node_ann.path_pref[1], PathPref.CUSTOMER)

        as3_paths = [[1, 4], [1, 2]]
        self.assertCountEqual(node_ann.best_paths[3], as3_paths)
        self.assertEqual(node_ann.path_pref[3], PathPref.PROVIDER)

        as7_paths = [[6, 4], [6, 2]]
        self.assertCountEqual(node_ann.best_paths[7], as7_paths)
        self.assertEqual(node_ann.path_pref[7], PathPref.PROVIDER)

        as11_paths = [[8, 5, 4], [9, 5, 4], [10, 5, 4]]
        self.assertCountEqual(node_ann.best_paths[11], as11_paths)
        self.assertEqual(node_ann.path_pref[11], PathPref.PROVIDER)

        self.assertEqual(node_ann.path_pref[12], PathPref.UNKNOWN)
        self.assertEqual(node_ann.path_pref[13], PathPref.UNKNOWN)

    def test_multiple_customer_sources(self):
        graph = _make_graph_multiple_choices()
        announce = Announcement.make_anycast_announcement(graph, [8, 10])
        node_ann = graph.infer_paths(announce)

        as11_paths = [[8,], [10,]]
        self.assertCountEqual(node_ann.best_paths[11], as11_paths)
        self.assertEqual(node_ann.path_pref[11], PathPref.PROVIDER)

        as13_paths = [[12, 8], [12, 10]]
        self.assertCountEqual(node_ann.best_paths[13], as13_paths)
        self.assertEqual(node_ann.path_pref[13], PathPref.PROVIDER)

        as9_paths = [[5, 8], [5, 10]]
        self.assertCountEqual(node_ann.best_paths[9], as9_paths)
        self.assertEqual(node_ann.path_pref[9], PathPref.PROVIDER)

        as1_paths = [
            [2, 5, 8],
            [3, 5, 8],
            [4, 5, 8],
            [2, 5, 10],
            [3, 5, 10],
            [4, 5, 10],
        ]
        self.assertCountEqual(node_ann.best_paths[1], as1_paths)
        self.assertEqual(node_ann.path_pref[1], PathPref.CUSTOMER)

        as7_paths = [
            [6, 2, 5, 8],
            [6, 3, 5, 8],
            [6, 4, 5, 8],
            [6, 2, 5, 10],
            [6, 3, 5, 10],
            [6, 4, 5, 10],
        ]
        self.assertCountEqual(node_ann.best_paths[7], as7_paths)
        self.assertEqual(node_ann.path_pref[7], PathPref.PROVIDER)

    def test_multiple_customer_sources_prepend(self):
        graph = _make_graph_multiple_choices()
        announce = Announcement.make_anycast_announcement(graph, [8, 10])
        announce.source2neighbor2path[8][5] = (8,) # type: ignore
        node_ann = graph.infer_paths(announce)

        as11_paths = [[8,], [10,]]
        self.assertCountEqual(node_ann.best_paths[11], as11_paths)
        self.assertEqual(node_ann.path_pref[11], PathPref.PROVIDER)

        as13_paths = [[12, 8], [12, 10]]
        self.assertCountEqual(node_ann.best_paths[13], as13_paths)
        self.assertEqual(node_ann.path_pref[13], PathPref.PROVIDER)

        as9_paths = [[5, 10]]
        self.assertCountEqual(node_ann.best_paths[9], as9_paths)
        self.assertEqual(node_ann.path_pref[9], PathPref.PROVIDER)

        as1_paths = [[2, 5, 10], [3, 5, 10], [4, 5, 10]]
        self.assertCountEqual(node_ann.best_paths[1], as1_paths)
        self.assertEqual(node_ann.path_pref[1], PathPref.CUSTOMER)

        as7_paths = [[6, 2, 5, 10], [6, 3, 5, 10], [6, 4, 5, 10]]
        self.assertCountEqual(node_ann.best_paths[7], as7_paths)
        self.assertEqual(node_ann.path_pref[7], PathPref.PROVIDER)

    def test_peer_peer_relationships(self):
        graph = _make_graph_peer_peer_relationships()
        g1 = graph.clone()

        announce = Announcement.make_anycast_announcement(graph, [2])
        node_ann = graph.infer_paths(announce)
        self.assertListEqual(node_ann.best_paths[9], [[1, 2]])
        self.assertEqual(node_ann.path_pref[9], PathPref.CUSTOMER)
        self.assertListEqual(node_ann.best_paths[6], [[5, 9, 1, 2]])
        self.assertEqual(node_ann.path_pref[6], PathPref.PROVIDER)
        self.assertListEqual(node_ann.best_paths[4], [[3, 1, 2]])
        self.assertEqual(node_ann.path_pref[4], PathPref.PROVIDER)
        self.assertEqual(node_ann.path_pref[7], PathPref.UNKNOWN)
        self.assertEqual(node_ann.path_pref[8], PathPref.UNKNOWN)
        self.assertEqual(node_ann.path_pref[10], PathPref.UNKNOWN)

        announce = Announcement.make_anycast_announcement(g1, [4])
        node_ann = g1.infer_paths(announce)
        self.assertListEqual(node_ann.best_paths[10], [[3, 4]])
        self.assertEqual(node_ann.path_pref[10], PathPref.CUSTOMER)
        self.assertListEqual(node_ann.best_paths[2], [[1, 3, 4]])
        self.assertEqual(node_ann.path_pref[2], PathPref.PROVIDER)
        self.assertListEqual(node_ann.best_paths[6], [[5, 3, 4]])
        self.assertEqual(node_ann.path_pref[6], PathPref.PROVIDER)
        self.assertListEqual(node_ann.best_paths[7], [[10, 3, 4]])
        self.assertEqual(node_ann.path_pref[7], PathPref.PROVIDER)
        self.assertListEqual(node_ann.best_paths[8], [[7, 10, 3, 4]])
        self.assertEqual(node_ann.path_pref[8], PathPref.PROVIDER)
        self.assertEqual(node_ann.path_pref[9], PathPref.UNKNOWN)

    def test_diamond_exhaustive(self):
        def make_three_way_diamond(relationship_combination):
            graph = ASGraph()
            graph.add_peering(1, 2, relationship_combination[0])
            graph.add_peering(1, 3, relationship_combination[1])
            graph.add_peering(1, 4, relationship_combination[2])
            graph.add_peering(2, 5, relationship_combination[3])
            graph.add_peering(3, 5, relationship_combination[4])
            graph.add_peering(4, 5, relationship_combination[5])
            return graph

        for relationship_combination in itertools.product(Relationship, repeat=6):
            graph = make_three_way_diamond(relationship_combination)
            announce = Announcement.make_anycast_announcement(graph, [1])
            node_ann = graph.infer_paths(announce)

            as5_paths = list()
            best_pref = PathPref.UNKNOWN
            for transit in [2, 3, 4]:
                as5_pref = PathPref.from_relationship(graph, transit, 5)
                if as5_pref < best_pref:
                    continue
                transit_pref = PathPref.from_relationship(graph, 1, transit)
                if transit_pref != PathPref.CUSTOMER and as5_pref != PathPref.PROVIDER:
                    # Route will not propagate to AS5
                    continue
                if as5_pref > best_pref:
                    as5_paths = [[transit, 1]]
                else:
                    as5_paths.append([transit, 1])
                best_pref = max(best_pref, as5_pref)

            self.assertCountEqual(node_ann.best_paths[5], as5_paths)
            self.assertEqual(node_ann.path_pref[5], best_pref)

    def test_peer_lock(self):
        graph = _make_graph_peer_lock()

        announce = Announcement.make_anycast_announcement(graph, [1, 7])
        node_ann = graph.infer_paths(announce)

        self.assertCountEqual(node_ann.best_paths[2], [[1,]])
        self.assertEqual(node_ann.path_pref[2], PathPref.PEER)
        self.assertCountEqual(node_ann.best_paths[4], [[1,]])
        self.assertEqual(node_ann.path_pref[4], PathPref.CUSTOMER)

        self.assertCountEqual(node_ann.best_paths[3], [[7,]])
        self.assertEqual(node_ann.path_pref[3], PathPref.CUSTOMER)
        self.assertCountEqual(node_ann.best_paths[5], [[7,], [1,]])
        self.assertEqual(node_ann.path_pref[5], PathPref.CUSTOMER)

        self.assertCountEqual(
            node_ann.best_paths[6], [[2, 1], [4, 1], [3, 7], [5, 7], [5, 1]]
        )
        self.assertEqual(node_ann.path_pref[6], PathPref.PROVIDER)

        self.assertCountEqual(
            node_ann.best_paths[8], [[4, 1], [3, 7], [5, 7], [5, 1]]
        )
        self.assertEqual(node_ann.path_pref[8], PathPref.PEER)

        self.assertCountEqual(
            node_ann.best_paths[9], [[4, 1], [3, 7], [5, 7], [5, 1]]
        )
        self.assertEqual(node_ann.path_pref[9], PathPref.CUSTOMER)


def workqueue_random_get(self, pref):
    TAIL_SHUFFLE = 5
    assert isinstance(self, WorkQueue)
    if not self.pref2depth2edge[pref]:
        return None
    depth = min(self.pref2depth2edge[pref])
    nedges = len(self.pref2depth2edge[pref][depth])
    index = random.randint(max(0, nedges - TAIL_SHUFFLE), nedges - 1)
    edge = self.pref2depth2edge[pref][depth][index]
    del self.pref2depth2edge[pref][depth][index]
    if not self.pref2depth2edge[pref][depth]:
        del self.pref2depth2edge[pref][depth]
    return edge


@unittest.skipIf(SLOW_TESTS_DISABLED, "WHY U NO RUST?!")
class TestCaidaASGraph(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        url = urllib.parse.urlparse(CAIDA_AS_RELATIONSHIPS_URL)
        db_filename = os.path.basename(url.path)
        db_filepath = os.path.join("tests", db_filename)
        if not os.path.exists(db_filepath):
            urllib.request.urlretrieve(CAIDA_AS_RELATIONSHIPS_URL, db_filepath)
        cls.graph = ASGraph.read_caida_asrel_graph(db_filepath)

    def setUp(self):
        self.graph = TestCaidaASGraph.graph.clone()

    def test_load_caida_asrel(self):
        self.assertIsNotNone(self.graph)

    @unittest.mock.patch.object(WorkQueue, "get", workqueue_random_get)
    def test_random_sources_on_caida_graph(self):
        SETS = 10
        ITERATIONS = 3

        for setnum in range(SETS):
            sources = random.sample(self.graph.g.nodes, 3)
            announce = Announcement.make_anycast_announcement(self.graph, sources)
            self.assertCountEqual(announce.source2neighbor2path.keys(), sources)

            g1 = self.graph.clone()
            node_ann1 = g1.infer_paths(announce)

            for iternum in range(ITERATIONS):
                print(f"source set {setnum}/{SETS}, iteration {iternum}/{ITERATIONS}")

                g2 = self.graph.clone()
                node_ann2 = g2.infer_paths(announce)

                for nodenum in g1.g.nodes:
                    n1_paths = node_ann1.best_paths[nodenum]
                    n2_paths = node_ann2.best_paths[nodenum]
                    self.assertCountEqual(n1_paths, n2_paths)
                    self.assertEqual(
                        node_ann1.path_pref[nodenum],
                        node_ann2.path_pref[nodenum],
                    )
