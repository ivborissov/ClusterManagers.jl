This draft version of condor.jl is the version that supports windows environment (both master and execute nodes)

Pre-requisites:
1. Julia is installed in the dir accessible by HTCondor
2. system variable JULIA_PKGDIR is set to the path where Julia is installed (ex. C:\Julia)
3. Ncat is installed on remote machines (nmap.org) to C:\\Program Files (x86)\\Nmap
4. '.julia-htc' dir is created in julia home (==JULIA_PKGDIR)
