<!--
 Copyright (c) 2021 dciangot

 This software is released under the MIT License.
 https://opensource.org/licenses/MIT
-->

# dask-remote-jobqueue

A custom Dask remote jobqueue for HTCondor, using [ssh forwarding via ssh-jhub-forwarder](https://github.com/comp-dev-cms-ita/ssh-jhub-forwarder) to enable remote worker nodes (running on a private network) to expose Dask scheduler services outside the network boundary.

The custom class spawn a Dask Scheduler inside the HTCondor cluster with a companion HTTP Controller to interact with
the Dask Cluster, for example, with actions like:

- start
- stop
- scale

The service connections are forwarded to the Dask Client and the Dask Scheduler interact directly with
the Dask Worker Nodes.

```text
                     SSH Forward               HTCondor Cluster
                       Service            ┌─────────────────┬──────────────┐
┌────────┐       ┌───────┬──┬───────┐     │                 │              │
│        │       ├───────┤  ├───────┤     │ Dask Scheduler  │◄──┐  ┌────┐  │
│        │       │       │  │       │◄────┤                 │   ├─►│ WN │  │
│ Dask   │       │ssh    │  │   ssh │     │      ┌──────────┤   │  └────┘  │
│ Client │◄──────┤listen │  │   fwd │◄────┼──────┤          │   │          │
│        │       │       │  │       │     │      │Dashboard │   │  ┌────┐  │
│        │       │       │  │       │◄──┐ ├──────┴──────────┤   ├─►│ WN │  │
└────────┘       ├───────┤  ├───────┤   │ │   ▲             │   │  └────┘  │
                 └───────┴──┴───────┘   │ │   │             │   │          │
                                        │ │   │             │   │  ┌────┐  │
                                        │ │   ▼             │   └─►│ WN │  │
                                        │ ├─────────────────┤      └────┘  │
                                        └─┤ HTTP Controller │              │
                                          └─────────────────┴──────────────┘
```
