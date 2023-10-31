precompile(Tuple{typeof(MultiscaleCluster.remotecall),Function,Int,Module,Vararg{Any, 100}})
precompile(Tuple{typeof(MultiscaleCluster.procs)})
precompile(Tuple{typeof(MultiscaleCluster.finalize_ref), MultiscaleCluster.Future})
# This is disabled because it doesn't give much benefit
# and the code in MultiscaleCluster is poorly typed causing many invalidations
# TODO: Maybe reenable now that MultiscaleCluster is not in sysimage.
#=
    precompile_script *= """
    using MultiscaleCluster
    addprocs(2)
    pmap(x->iseven(x) ? 1 : 0, 1:4)
    @distributed (+) for i = 1:100 Int(rand(Bool)) end
    """
=#
