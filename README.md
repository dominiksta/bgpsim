# bgpsim

BGP path propagation inference

## What this program does

Starting from a graph of all ASes on the internet and their business relationships (transit or peering), BGPSim simulates the likely propagation of a BGP announcement through the internet.

A typical use case of this might be to get a mostly reasonable AS path from A to B with relatively minimal effort and easily available public data.

To populate the business relationships, BGPSim can read the CAIDA AS relationship dataset.

BGPSim's main algorithm traverses ASes in a similar way to breadth-first search. During the traversal of the AS graph, the given announcement is propagated using the following contraints:
- All generated routes must be valley free (That is, no as will propagate routes
  received from its peers or providers to a peer or provider, in order to not
  offer transit for free.)
- Routes with cycles will be discarded

## Usage

While BGPSim was originally developed for python3.8, it also works on pypy3.8,
which you might prefer to use for better performance. Regardless, the
development setup is the same.

### Setup

```bash
pypy3.8 -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
```

### Example Program

(Disabling assertions with `-O` makes the code significantly faster as it is pretty heavy on asserts.)

```bash
$ python -O -m examples.simple
Reading AS relationship file...
Done reading AS relationship file, read 67306 ASes
Computing likely paths to AS39063 on the internet...
Done computing paths, took 0.774s
[(6939, 39063)]
```

### Basic benchmarking

You can check the runtime of random path inferences on CAIDA's January 2020 graph by using the `tests/bench_bgpsim.py` script. It reports 5 averages over 32 full inference runs each. 

``` {bash}
$ python3 tests/bench_bgpsim.py
[1065.9921099510975, 1129.7197931839619, 1341.9510222299723, 1212.2649150219513, 1117.318360270001]
$ python3 -O tests/bench_bgpsim.py
[244.108187089907, 246.5742465169169, 244.00426896300633, 235.13479439693037, 255.45060764998198]
```

## References

You man want to check these papers on an [introduction to BGP routing policies][bgp-policies], and on [how policies can be inferred in the wild][caida-asrel].

## TO-DO

* Write tests for poisoned announcements. The code should work for announcements with poisoning, but there are no tests for this functionality yet.

[bgp-policies]: https://doi.org/10.1109/MNET.2005.1541715
[caida-asrel]: https://doi.org/10.1145/2504730.2504735