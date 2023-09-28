from dask_remote_jobqueue import RemoteHTCondor

def main():
    cluster = RemoteHTCondor(
            user = "dciangot",
            ssh_url = "cms-it-hub.cloud.cnaf.infn.it",
            ssh_url_port = 31023,
            sitename = "T2_LNL_PD_CloudVeneto",
            singularity_wn_image = "/cvmfs/images.dodas.infn.it/registry.hub.docker.com/dodasts/root-in-docker:ubuntu22-kernel-v1",
            asynchronous = False,
            debug = False
    )

    cluster.start()

    print(cluster.scheduler_info)
    
    cluster.close()

if __name__=='__main__':
        main()
