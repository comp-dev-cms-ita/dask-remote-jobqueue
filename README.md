<!--
 Copyright (c) 2021 dciangot

 This software is released under the MIT License.
 https://opensource.org/licenses/MIT
-->

# dask-remote-jobqueue

A custom dask remote jobqueue for HTCondor, using [ssh forwarding via ssh-jhub-forwarder](https://github.com/comp-dev-cms-ita/ssh-jhub-forwarder) to enable remote worker nodes (running on private network) to expose Dask scheduler services outside the network boundary.

```text
                                                                    +----------------------------------+
                                                                    |                                  |
                                                                    |                                  |
                                                                    |    HTCondor Cluster              |
                               +---------------------+              |                                  |
                               |                     |              |                                  |
+--------------+               +--------+   +--------+              |             +--------------+     |
|              |               |        |   |        |              |             |  DASK        |     |
| Dask         <---------------+ ssh    <---+ ssh    +--------------+-------------+    Scheduler |     |
|    Client    |               | listen |   | fwd    |              |             |              |     |
|              |               |        |   |        |              |             |         WN   |     |
+--------------+               +--------+   +--------+              |    +----+   +--------------+     |
                               |                     |              |    | WN |                        |
                               +---------------------+              |    +----+   +----+       +----+  |
                                                                    |             | WN |       | WN |  |
                                                                    |             +----+       +----+  |
                                                                    |                                  |
                                                                    +----------------------------------+
``` 
