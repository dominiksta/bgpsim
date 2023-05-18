import os
import time
from typing import List, Tuple
import urllib.parse
import urllib.request
from pathlib import Path

from bgpsim import NODE_BEST_PATHS, ASGraph, Announcement

_graph = None

def get_graph():
    global _graph

    def get_caida_asrel_file():
        url = "http://data.caida.org/datasets/as-relationships/serial-1/" + \
            "20200101.as-rel.txt.bz2"
        outfile = Path(os.path.dirname(__file__)) / 'asrel.txt.bz2'
        if not os.path.exists(outfile):
            print('Downloading AS relationship file')
            urllib.request.urlretrieve(url, outfile)
        return outfile

    if _graph is None:
        print('Reading AS relationship file...')
        _graph = ASGraph.read_caida_asrel_graph(get_caida_asrel_file())
        print(f'Done reading AS relationship file, read {len(_graph.g.nodes)} ASes')

    return _graph.clone()

def compute_likely_paths(source: int, sink: int) -> List[Tuple[int]]:
    graph = get_graph()

    announcement = Announcement.make_anycast_announcement(graph, [ source ])
    before = time.time()
    print(f'Computing likely paths to AS{source} on the internet...')
    graph.infer_paths(announcement)
    print(f'Done computing paths, took {round(time.time() - before, 3)}s')

    return graph.g.nodes[sink][NODE_BEST_PATHS]

def main():
    print(compute_likely_paths(
        39063, # Leitwert
        56357, # TUM-I8-AS Technische Universitaet Muenchen, DE
    ))

if __name__ == '__main__': main()