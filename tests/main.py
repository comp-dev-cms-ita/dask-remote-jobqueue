from dask_remote_jobqueue import RemoteHTCondor

cluster = RemoteHTCondor(
    user = "dciangot",
    ssh_url = "localhost",
    ssh_url_port = 3081,
    sitename = "",
    singularity_wn_image = "/cvmfs/images.dodas.infn.it/registry.hub.docker.com/dodasts/root-in-docker:ubuntu22-kernel-v1",
    asynchronous = False
)


cluster.start()

cluster.job_status