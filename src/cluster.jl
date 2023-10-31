# This file is a part of Julia. License is MIT: https://julialang.org/license

"""
    ClusterManager

Supertype for cluster managers, which control workers processes as a cluster.
Cluster managers implement how workers can be added, removed and communicated with.
`SSHManager` and `LocalManager` are subtypes of this.
"""
abstract type ClusterManager end

"""
    WorkerConfig

Type used by [`ClusterManager`](@ref)s to control workers added to their clusters. Some fields
are used by all cluster managers to access a host:
  * `io` -- the connection used to access the worker (a subtype of `IO` or `Nothing`)
  * `host` -- the host address (either a `String` or `Nothing`)
  * `port` -- the port on the host used to connect to the worker (either an `Int` or `Nothing`)

Some are used by the cluster manager to add workers to an already-initialized host:
  * `count` -- the number of workers to be launched on the host
  * `exename` -- the path to the Julia executable on the host, defaults to `"\$(Sys.BINDIR)/julia"` or
    `"\$(Sys.BINDIR)/julia-debug"`
  * `exeflags` -- flags to use when launching Julia remotely

The `userdata` field is used to store information for each worker by external managers.

Some fields are used by `SSHManager` and similar managers:
  * `tunnel` -- `true` (use tunneling), `false` (do not use tunneling), or [`nothing`](@ref) (use default for the manager)
  * `multiplex` -- `true` (use SSH multiplexing for tunneling) or `false`
  * `forward` -- the forwarding option used for `-L` option of ssh
  * `bind_addr` -- the address on the remote host to bind to
  * `sshflags` -- flags to use in establishing the SSH connection
  * `max_parallel` -- the maximum number of workers to connect to in parallel on the host

Some fields are used by both `LocalManager`s and `SSHManager`s:
  * `connect_at` -- determines whether this is a worker-to-worker or driver-to-worker setup call
  * `process` -- the process which will be connected (usually the manager will assign this during [`addprocs`](@ref))
  * `ospid` -- the process ID according to the host OS, used to interrupt worker processes
  * `environ` -- private dictionary used to store temporary information by Local/SSH managers
  * `ident` -- worker as identified by the [`ClusterManager`](@ref)
  * `connect_idents` -- list of worker ids the worker must connect to if using a custom topology
  * `enable_threaded_blas` -- `true`, `false`, or `nothing`, whether to use threaded BLAS or not on the workers
"""
mutable struct WorkerConfig
    # Common fields relevant to all cluster managers
    io::Union{IO, Nothing}
    host::Union{String, Nothing}
    port::Union{Int, Nothing}

    # Used when launching additional workers at a host
    count::Union{Int, Symbol, Nothing}
    exename::Union{String, Cmd, Nothing}
    exeflags::Union{Cmd, Nothing}

    # External cluster managers can use this to store information at a per-worker level
    # Can be a dict if multiple fields need to be stored.
    userdata::Any

    # SSHManager / SSH tunnel connections to workers
    tunnel::Union{Bool, Nothing}
    multiplex::Union{Bool, Nothing}
    forward::Union{String, Nothing}
    bind_addr::Union{String, Nothing}
    sshflags::Union{Cmd, Nothing}
    max_parallel::Union{Int, Nothing}

    # Used by Local/SSH managers
    connect_at::Any

    process::Union{Process, Nothing}
    ospid::Union{Int, Nothing}

    # Private dictionary used to store temporary information by Local/SSH managers.
    environ::Union{Dict, Nothing}

    # Connections to be setup depending on the network topology requested
    ident::Any      # Worker as identified by the Cluster Manager.
    # List of other worker idents this worker must connect with. Used with topology T_CUSTOM.
    connect_idents::Union{Array, Nothing}

    # Run multithreaded blas on worker
    enable_threaded_blas::Union{Bool, Nothing}

    function WorkerConfig()
        wc = new()
        for n in 1:fieldcount(WorkerConfig)
            setfield!(wc, n, nothing)
        end
        wc
    end
end

@enum WorkerState W_CREATED W_CONNECTED W_TERMINATING W_TERMINATED
mutable struct Worker
    id::Int
    msg_lock::Threads.ReentrantLock # Lock for del_msgs, add_msgs, and gcflag
    del_msgs::Array{Any,1} # XXX: Could del_msgs and add_msgs be Channels?
    add_msgs::Array{Any,1}
    @atomic gcflag::Bool
    state::WorkerState
    c_state::Condition      # wait for state changes
    ct_time::Float64        # creation time
    conn_func::Any          # used to setup connections lazily

    r_stream::IO
    w_stream::IO
    w_serializer::ClusterSerializer  # writes can happen from any task hence store the
                                     # serializer as part of the Worker object
    manager::ClusterManager
    config::WorkerConfig
    version::Union{VersionNumber, Nothing}   # Julia version of the remote process
    initialized::Event

    function Worker(id::Int, r_stream::IO, w_stream::IO, manager::ClusterManager;
                             version::Union{VersionNumber, Nothing}=nothing,
                             config::WorkerConfig=WorkerConfig(), role= :default)
        w = Worker(id; role = role)
        w.r_stream = r_stream
        w.w_stream = buffer_writes(w_stream)
        w.w_serializer = ClusterSerializer(w.w_stream)
        w.manager = manager
        w.config = config
        w.version = version
        set_worker_state(w, W_CONNECTED)
        register_worker_streams(w)
        w
    end

    Worker(id::Int; role= :default) = Worker(id, nothing; role = role)
    function Worker(id::Int, conn_func; role= :default)
        @assert id > 0
        map_pid_wrkr = Map_pid_wrkr(role = role)
        if haskey(map_pid_wrkr, id)
            return map_pid_wrkr[id]
        end
        w=new(id, Threads.ReentrantLock(), [], [], false, W_CREATED, Condition(), time(), conn_func)
        w.initialized = Event()
        register_worker(w; role = role)
        w
    end

    Worker(;role= :default) = Worker(get_next_pid(); role = role)
end

id(w::Worker; role= :default) = w.id

function set_worker_state(w, state)
    w.state = state
    notify(w.c_state; all=true)
end

function check_worker_state(w::Worker; role= :default)
    if w.state === W_CREATED
        if !isclusterlazy(role = role)
            pg = PGRP(role = role)
            if pg.topology === :all_to_all
                # Since higher pids connect with lower pids, the remote worker
                # may not have connected to us yet. Wait for some time.
                wait_for_conn(w; role=role)
            else
                error("peer $(id(w, role=role)) is not connected to $(myid(role=role)). Topology : " * string(pg.topology))
            end
        else
            w.ct_time = time()
            if myid(role=role) > id(w, role=role)
                t = @async exec_conn_func(w; role=role)
            else
                # route request via node 1
                t = @async remotecall_fetch((p,to_id) -> remotecall_fetch(to_id -> exec_conn_func(to_id, role = p == 1 ? :manager : :worker), p, to_id; role = role), 1, id(w, role=role), myid(role=role))
            end
            errormonitor(t)
            wait_for_conn(w; role=role)
        end
    end
end

exec_conn_func(id::Int; role= :default) = exec_conn_func(worker_from_id(id; role = role)::Worker; role = role)
function exec_conn_func(w::Worker; role= :default)
    try
        f = notnothing(w.conn_func)
        # Will be called if some other task tries to connect at the same time.
        w.conn_func = () -> wait_for_conn(w; role=role)
        f()
    catch e
        w.conn_func = () -> throw(e)
        rethrow()
    end
    nothing
end

function wait_for_conn(w; role=:defaut)
    if w.state === W_CREATED
        timeout =  worker_timeout() - (time() - w.ct_time)
        timeout <= 0 && error("peer $(id(w, role=role)) has not connected to $(myid(role=role))")

        @async (sleep(timeout); notify(w.c_state; all=true))
        wait(w.c_state)
        w.state === W_CREATED && error("peer $(id(w, role=role)) didn't connect to $(myid(role=role)) within $timeout seconds")
    end
    nothing
end

## process group creation ##

mutable struct LocalProcess
    id0::Int
    id1::Int
    bind_addr::String
    bind_port::UInt16
    cookie::String
    LocalProcess() = new(1,1)
end

function id(lp::LocalProcess; role= :default) 
    if role == :manager 
        return lp.id1
    elseif role == :worker 
        return lp.id0
    elseif role == :default && myrole() == :master
        return lp.id1 # as :manager
    elseif role == :default && mrole() == :worker
        return lp.id0 # as :worker
    else
        throw("unexpected use of role=:default (id)")
    end

end

worker_timeout() = parse(Float64, get(ENV, "JULIA_WORKER_TIMEOUT", "60.0"))


## worker creation and setup ##
"""
    start_worker([out::IO=stdout], cookie::AbstractString=readline(stdin); close_stdin::Bool=true, stderr_to_stdout::Bool=true)

`start_worker` is an internal function which is the default entry point for
worker processes connecting via TCP/IP. It sets up the process as a Julia cluster
worker.

host:port information is written to stream `out` (defaults to stdout).

The function reads the cookie from stdin if required, and  listens on a free port
(or if specified, the port in the `--bind-to` command line option) and schedules
tasks to process incoming TCP connections and requests. It also (optionally)
closes stdin and redirects stderr to stdout.

It does not return.
"""
start_worker(cookie::AbstractString=readline(stdin); kwargs...) = start_worker(stdout, cookie; kwargs...)
function start_worker(out::IO, cookie::AbstractString=readline(stdin); close_stdin::Bool=true, stderr_to_stdout::Bool=true)
    @info :start_worker
    init_multi()

    if close_stdin # workers will not use it
        redirect_stdin(devnull)
        close(stdin)
    end
    stderr_to_stdout && redirect_stderr(stdout)

    init_worker(cookie)
    interface = IPv4(LPROC.bind_addr)
    if LPROC.bind_port == 0
        port_hint = 9000 + (getpid() % 1000)
        (port, sock) = listenany(interface, UInt16(port_hint))
        LPROC.bind_port = port
    else
        sock = listen(interface, LPROC.bind_port)
    end
    errormonitor(@async while isopen(sock)
        client = accept(sock)
        process_messages(client, client, true; role = :worker)
    end)
    println(out, "julia_worker:$(string(LPROC.bind_port))#$(LPROC.bind_addr)\n")  # print header
    flush(out)

    Sockets.nagle(sock, false)
    Sockets.quickack(sock, true)

    if ccall(:jl_running_on_valgrind,Cint,()) != 0
        println(out, "PID = $(getpid())")
    end

    try
        # To prevent hanging processes on remote machines, newly launched workers exit if the
        # master process does not connect in time.
        check_master_connect()
        while true; wait(); end
    catch err
        print(stderr, "unhandled exception on $(myid(role = :worker)): $(err)\nexiting.\n")
    end

    close(sock)
    exit(0)
end


function redirect_worker_output(ident, stream)
    t = @async while !eof(stream)
        line = readline(stream)
        if startswith(line, "      From worker ")
            # stdout's of "additional" workers started from an initial worker on a host are not available
            # on the master directly - they are routed via the initial worker's stdout.
            println(line)
        else
            println("      From worker $(ident):\t$line")
        end
    end
    errormonitor(t)
end

struct LaunchWorkerError <: Exception
    msg::String
end

Base.showerror(io::IO, e::LaunchWorkerError) = print(io, e.msg)

# The default TCP transport relies on the worker listening on a free
# port available and printing its bind address and port.
# The master process uses this to connect to the worker and subsequently
# setup a all-to-all network.
function read_worker_host_port(io::IO)
    t0 = time_ns()

    # Wait at most for JULIA_WORKER_TIMEOUT seconds to read host:port
    # info from the worker
    timeout = worker_timeout() * 1e9
    # We expect the first line to contain the host:port string. However, as
    # the worker may be launched via ssh or a cluster manager like SLURM,
    # ignore any informational / warning lines printed by the launch command.
    # If we do not find the host:port string in the first 1000 lines, treat it
    # as an error.

    ntries = 1000
    leader = String[]
    try
        while ntries > 0
            readtask = @async readline(io)
            yield()
            while !istaskdone(readtask) && ((time_ns() - t0) < timeout)
                sleep(0.05)
            end
            !istaskdone(readtask) && break

            conninfo = fetch(readtask)
            if isempty(conninfo) && !isopen(io)
                throw(LaunchWorkerError("Unable to read host:port string from worker. Launch command exited with error?"))
            end

            ntries -= 1
            bind_addr, port = parse_connection_info(conninfo)
            if !isempty(bind_addr)
                return bind_addr, port
            end

            # collect unmatched lines
            push!(leader, conninfo)
        end
        close(io)
        if ntries > 0
            throw(LaunchWorkerError("Timed out waiting to read host:port string from worker."))
        else
            throw(LaunchWorkerError("Unexpected output from worker launch command. Host:port string not found."))
        end
    finally
        for line in leader
            println("\tFrom worker startup:\t", line)
        end
    end
end

function parse_connection_info(str)
    m = match(r"^julia_worker:(\d+)#(.*)", str)
    if m !== nothing
        (String(m.captures[2]), parse(UInt16, m.captures[1]))
    else
        ("", UInt16(0))
    end
end

"""
    init_worker(cookie::AbstractString, manager::ClusterManager=DefaultClusterManager())

Called by cluster managers implementing custom transports. It initializes a newly launched
process as a worker. Command line argument `--worker[=<cookie>]` has the effect of initializing a
process as a worker using TCP/IP sockets for transport.
`cookie` is a [`cluster_cookie`](@ref).
"""
function init_worker(cookie::AbstractString, manager::ClusterManager=DefaultClusterManager())
    myrole!(:worker)

    # On workers, the default cluster manager connects via TCP sockets. Custom
    # transports will need to call this function with their own manager.
    global cluster_manager
    cluster_manager = manager

    # Since our pid has yet to be set, ensure no RemoteChannel / Future  have been created or addprocs() called.
    @assert nprocs() <= 1
    @assert isempty(PGRP(role = :worker).refs)
    @assert isempty(client_refs)

    # System is started in head node mode, cleanup related entries
    empty!(PGRP(role = :worker).workers)
    empty!(Map_pid_wrkr(role = :worker))

    cluster_cookie(cookie)
    nothing
end


# The main function for adding worker processes.
# `manager` is of type ClusterManager. The respective managers are responsible
# for launching the workers. All keyword arguments (plus a few default values)
# are available as a dictionary to the `launch` methods
#
# Only one addprocs can be in progress at any time
#
const worker_lock = ReentrantLock()

"""
    addprocs(manager::ClusterManager; kwargs...) -> List of process identifiers

Launches worker processes via the specified cluster manager.

For example, Beowulf clusters are supported via a custom cluster manager implemented in
the package `ClusterManagers.jl`.

The number of seconds a newly launched worker waits for connection establishment from the
master can be specified via variable `JULIA_WORKER_TIMEOUT` in the worker process's
environment. Relevant only when using TCP/IP as transport.

To launch workers without blocking the REPL, or the containing function
if launching workers programmatically, execute `addprocs` in its own task.

# Examples

```julia
# On busy clusters, call `addprocs` asynchronously
t = @async addprocs(...)
```

```julia
# Utilize workers as and when they come online
if nprocs() > 1   # Ensure at least one new worker is available
   ....   # perform distributed execution
end
```

```julia
# Retrieve newly launched worker IDs, or any error messages
if istaskdone(t)   # Check if `addprocs` has completed to ensure `fetch` doesn't block
    if nworkers() == N
        new_pids = fetch(t)
    else
        fetch(t)
    end
end
```
"""
function addprocs(manager::ClusterManager; kwargs...)
    init_multi()

#    cluster_mgmt_from_master_check()

    lock(worker_lock)
    try

        if myrole() == :worker
            mrole!(:manager_worker)
        end
        PGRP(role=:manager).level = PGRP(role=:worker).level + 1

        addprocs_locked(manager::ClusterManager; kwargs...)
    finally
        unlock(worker_lock)
    end
end

function addprocs_locked(manager::ClusterManager; kwargs...)
    params = merge(default_addprocs_params(manager), Dict{Symbol,Any}(kwargs))
    topology(Symbol(params[:topology]); role = :manager)

    pgm = PGRP(role = :manager) 

    if pgm.topology !== :all_to_all
        params[:lazy] = false
    end

    if pgm.lazy === nothing || nprocs() == 1
        pgm.lazy = params[:lazy]
    elseif isclusterlazy(role = :manager) != params[:lazy]
        throw(ArgumentError(string("Active workers with lazy=", isclusterlazy(role = :manager),
                                    ". Cannot set lazy=", params[:lazy])))
    end

    # References to launched workers, filled when each worker is fully initialized and
    # has connected to all nodes.
    launched_q = Int[]   # Asynchronously filled by the launch method

    # The `launch` method should add an object of type WorkerConfig for every
    # worker launched. It provides information required on how to connect
    # to it.

    # FIXME: launched should be a Channel, launch_ntfy should be a Threads.Condition
    # but both are part of the public interface. This means we currently can't use
    # `Threads.@spawn` in the code below.
    launched = WorkerConfig[]
    launch_ntfy = Condition()

    # call manager's `launch` is a separate task. This allows the master
    # process initiate the connection setup process as and when workers come
    # online
    t_launch = @async launch(manager, params, launched, launch_ntfy)

    @sync begin
        while true
            if isempty(launched)
                istaskdone(t_launch) && break
                @async (sleep(1); notify(launch_ntfy))
                wait(launch_ntfy)
            end

            if !isempty(launched)
                wconfig = popfirst!(launched)
                let wconfig=wconfig
                    @async setup_launched_worker(manager, wconfig, launched_q)
                end
            end
        end
    end

    Base.wait(t_launch)      # catches any thrown errors from the launch task

    # Since all worker-to-worker setups may not have completed by the time this
    # function returns to the caller, send the complete list to all workers.
    # Useful for nprocs(), nworkers(), etc to return valid values on the workers.
    all_w = workers(role = :manager)
    for pid in all_w
        remote_do(all_w -> set_valid_processes(all_w, role = pid == 1 ? :manager : :worker), pid, all_w; role = :manager)
    end

    sort!(launched_q)
end

function set_valid_processes(plist::Array{Int}; role= :default)
    for pid in setdiff(plist, workers())
        myid(role=role) != pid && Worker(pid; role = role)
    end
end

"""
    default_addprocs_params(mgr::ClusterManager) -> Dict{Symbol, Any}

Implemented by cluster managers. The default keyword parameters passed when calling
`addprocs(mgr)`. The minimal set of options is available by calling
`default_addprocs_params()`
"""
default_addprocs_params(::ClusterManager) = default_addprocs_params()
default_addprocs_params() = Dict{Symbol,Any}(
    :topology => :all_to_all,
    :dir      => pwd(),
    :exename  => joinpath(Sys.BINDIR, julia_exename()),
    :exeflags => ``,
    :env      => [],
    :enable_threaded_blas => false,
    :lazy => true)


function setup_launched_worker(manager, wconfig, launched_q)
    pid = create_worker(manager, wconfig)
    push!(launched_q, pid)

    # When starting workers on remote multi-core hosts, `launch` can (optionally) start only one
    # process on the remote machine, with a request to start additional workers of the
    # same type. This is done by setting an appropriate value to `WorkerConfig.cnt`.
    cnt = something(wconfig.count, 1)
    if cnt === :auto
        cnt = wconfig.environ[:cpu_threads]
    end
    cnt = cnt - 1   # Removing self from the requested number

    if cnt > 0
        launch_n_additional_processes(manager, pid, wconfig, cnt, launched_q)
    end
end


function launch_n_additional_processes(manager, frompid, fromconfig, cnt, launched_q)
    @sync begin
        exename = notnothing(fromconfig.exename)
        exeflags = something(fromconfig.exeflags, ``)
        cmd = `$exename $exeflags`

        new_addresses = remotecall_fetch(launch_additional, frompid, cnt, cmd; role = :manager)
        for address in new_addresses
            (bind_addr, port) = address

            wconfig = WorkerConfig()
            for x in [:host, :tunnel, :multiplex, :sshflags, :exeflags, :exename, :enable_threaded_blas]
                Base.setproperty!(wconfig, x, Base.getproperty(fromconfig, x))
            end
            wconfig.bind_addr = bind_addr
            wconfig.port = port

            let wconfig=wconfig
                @async begin
                    pid = create_worker(manager, wconfig)
                    remote_do(redirect_output_from_additional_worker, frompid, pid, port; role = :manager)
                    push!(launched_q, pid)
                end
            end
        end
    end
end

function create_worker(manager, wconfig)
    role = :manager

    @info :begin, :create_worker

    # only node 1 can add new nodes, since nobody else has the full list of address:port
    @assert myid(role=role) == 1
    timeout = worker_timeout()

    # initiate a connect. Does not wait for connection completion in case of TCP.
    w = Worker(role = role)
    local r_s, w_s
    try
        (r_s, w_s) = connect(manager, id(w, role=role), wconfig)
    catch ex
        try
            deregister_worker(id(w, role=role), role = role)
            kill(manager, id(w, role=role), wconfig)
        finally
            rethrow(ex)
        end
    end

    w = Worker(id(w, role=role), r_s, w_s, manager; config=wconfig, role = role)
    # install a finalizer to perform cleanup if necessary
    finalizer(w) do w
        if myid(role=role) == 1
            manage(w.manager, id(w, role=role), w.config, :finalize)
        end
    end

    # set when the new worker has finished connections with all other workers
    ntfy_oid = RRID(role = role)
    rr_ntfy_join = lookup_ref(ntfy_oid; role = role)
    rr_ntfy_join.waitingfor = myid(role=role)

    # Start a new task to handle inbound messages from connected worker in master.
    # Also calls `wait_connected` on TCP streams.
    process_messages(w.r_stream, w.w_stream, false; role = :manager)

    # send address information of all workers to the new worker.
    # Cluster managers set the address of each worker in `WorkerConfig.connect_at`.
    # A new worker uses this to setup an all-to-all network if topology :all_to_all is specified.
    # Workers with higher pids connect to workers with lower pids. Except process 1 (master) which
    # initiates connections to all workers.

    # Connection Setup Protocol:
    # - Master sends 16-byte cookie followed by 16-byte version string and a JoinPGRP message to all workers
    # - On each worker
    #   - Worker responds with a 16-byte version followed by a JoinCompleteMsg
    #   - Connects to all workers less than its pid. Sends the cookie, version and an IdentifySocket message
    #   - Workers with incoming connection requests write back their Version and an IdentifySocketAckMsg message
    # - On master, receiving a JoinCompleteMsg triggers rr_ntfy_join (signifies that worker setup is complete)

    join_list = []
    pgm = PGRP(role = role)
    if pgm.topology === :all_to_all
        # need to wait for lower worker pids to have completed connecting, since the numerical value
        # of pids is relevant to the connection process, i.e., higher pids connect to lower pids and they
        # require the value of config.connect_at which is set only upon connection completion
        for jw in pgm.workers
            if (id(jw, role=role) != 1) && (id(jw, role=role) < id(w, role=role))
                (jw.state === W_CREATED) && wait(jw.c_state)
                push!(join_list, jw)
            end
        end

    elseif pgm.topology === :custom
        # wait for requested workers to be up before connecting to them.
        filterfunc(x) = (id(x, role=role) != 1) && isdefined(x, :config) &&
            (notnothing(x.config.ident) in something(wconfig.connect_idents, []))

        wlist = filter(filterfunc, pgm.workers)
        waittime = 0
        while wconfig.connect_idents !== nothing &&
              length(wlist) < length(wconfig.connect_idents)
            if waittime >= timeout
                error("peer workers did not connect within $timeout seconds")
            end
            sleep(1.0)
            waittime += 1
            wlist = filter(filterfunc, pgm.workers)
        end

        for wl in wlist
            (wl.state === W_CREATED) && wait(wl.c_state)
            push!(join_list, wl)
        end
    end

    all_locs = mapany(x -> isa(x, Worker) ?
                      (something(x.config.connect_at, ()), id(x, role=role)) :
                      ((), id(x, role=role), true),
                      join_list)
    send_connection_hdr(w, true)
    enable_threaded_blas = something(wconfig.enable_threaded_blas, false)
    join_message = JoinPGRPMsg(id(w, role=role), all_locs, pgm.topology, enable_threaded_blas, isclusterlazy(role = role))
    send_msg_now(w, MsgHeader(RRID(0,0), ntfy_oid), join_message; role = role)

    @async manage(w.manager, id(w, role=role), w.config, :register)
    # wait for rr_ntfy_join with timeout
    timedout = false
    @async (sleep($timeout); timedout = true; put!(rr_ntfy_join, 1))
    wait(rr_ntfy_join)
    if timedout
        error("worker did not connect within $timeout seconds")
    end
    lock(client_refs) do
        delete!(pgm.refs, ntfy_oid)
    end

    @info :end, :create_worker

    return id(w, role=role)
end


# Called on the first worker on a remote host. Used to optimize launching
# of multiple workers on a remote host (to leverage multi-core)

additional_io_objs=Dict()
function launch_additional(np::Integer, cmd::Cmd)
    io_objs = Vector{Any}(undef, np)
    addresses = Vector{Any}(undef, np)

    for i in 1:np
        io = open(detach(cmd), "r+")
        write_cookie(io)
        io_objs[i] = io.out
    end

    for (i,io) in enumerate(io_objs)
        (host, port) = read_worker_host_port(io)
        addresses[i] = (host, port)
        additional_io_objs[port] = io
    end

    return addresses
end

function redirect_output_from_additional_worker(pid, port)
    io = additional_io_objs[port]
    redirect_worker_output("$pid", io)
    delete!(additional_io_objs, port)
    nothing
end

function check_master_connect()
    timeout = worker_timeout() * 1e9
    # If we do not have at least process 1 connect to us within timeout
    # we log an error and exit, unless we're running on valgrind
    if ccall(:jl_running_on_valgrind,Cint,()) != 0
        return
    end
    @async begin
        map_pid_wrkr = Map_pid_wrkr(role = :worker)
        start = time_ns()
        while !haskey(map_pid_wrkr, 1) && (time_ns() - start) < timeout
            sleep(1.0)
        end

        if !haskey(map_pid_wrkr, 1)
            print(stderr, "Master process (id 1) could not connect within $(timeout/1e9) seconds.\nexiting.\n")
            exit(1)
        end
    end
end


"""
    cluster_cookie() -> cookie

Return the cluster cookie.
"""
cluster_cookie() = (init_multi(); LPROC.cookie)

"""
    cluster_cookie(cookie) -> cookie

Set the passed cookie as the cluster cookie, then returns it.
"""
function cluster_cookie(cookie)
    init_multi()
    # The cookie must be an ASCII string with length <=  HDR_COOKIE_LEN
    @assert isascii(cookie)
    @assert length(cookie) <= HDR_COOKIE_LEN

    cookie = rpad(cookie, HDR_COOKIE_LEN)

    LPROC.cookie = cookie
    cookie
end


let next_pid = 2    # 1 is reserved for the client (always)
    global get_next_pid
    function get_next_pid()
        retval = next_pid
        next_pid += 1
        retval
    end
end

mutable struct ProcessGroup
    level::Integer
    name::String
    workers::Array{Any,1}
    refs::Dict{RRID,Any}                  # global references
    topology::Symbol
    lazy::Union{Bool, Nothing}

    ProcessGroup(w::Array{Any,1}) = new(0, "pg-default", w, Dict(), :all_to_all, nothing)
end

const _PGRP0 = ProcessGroup([])
const _PGRP1 = ProcessGroup([])

function PGRP(;role= :default)
    if role == :manager 
#        @info "$(role) / PGRP1 !"
        return _PGRP1
    elseif role == :worker 
#        @info "$(role) / PGRP0 ! -- worker"
        return _PGRP0
#    elseif role == :default && _PGRP0.level == 0
    elseif role == :default && myrole() == :master
#        @info "$(role) / PGRP1 !"
        return _PGRP1 # as :manager
#    elseif role == :default && _PGRP0.level > 0
    elseif role == :default && myrole() == :worker
#        @info "$(role) / PGRP0 !"
        return _PGRP0 # as :worker
    else
        throw("unexpected use of role = :default (PGRP)")
    end
end

function topology(t; role= :default)
    @assert t in [:all_to_all, :master_worker, :custom]
    pg = PGRP(role = role)
    if (pg.topology==t) || ((myid(role=role)==1) && (nprocs()==1)) || (myid(role=role) > 1)
        pg.topology = t
    else
        error("Workers with Topology $(pg.topology) already exist. Requested Topology $(t) cannot be set.")
    end
    t
end

isclusterlazy(; role= :default) = something(PGRP(role = role).lazy, false)

get_bind_addr(pid::Integer) = get_bind_addr(worker_from_id(pid; role = :manager))  # always called as manager 
get_bind_addr(w::LocalProcess) = LPROC.bind_addr                                   # always called as manager  
function get_bind_addr(w::Worker)       
    role = :worker                                           # always called as worker
    if w.config.bind_addr === nothing
        if id(w, role=role) != myid(role=role)
            w.config.bind_addr = remotecall_fetch(get_bind_addr, id(w, role=role), id(w, role=role), role = role)
        end
    end
    w.config.bind_addr
end

# globals
const LPROC = LocalProcess()
const LPROCROLE = Ref{Symbol}(:master)
const HDR_VERSION_LEN=16
const HDR_COOKIE_LEN=16
const _map_pid_wrkr_0 = Dict{Int, Union{Worker, LocalProcess}}()
const _map_pid_wrkr_1 = Dict{Int, Union{Worker, LocalProcess}}()
const map_sock_wrkr = IdDict()
const map_del_wrkr = Set{Int}()

function Map_pid_wrkr(;role= :default)
   # @info ("_map_pid_wrkr_0", _map_pid_wrkr_0, "end")
   # @info ("_map_pid_wrkr_1", _map_pid_wrkr_1, "end")
    pg = PGRP(role = role)
    if role == :manager 
    #    @info "Map_pid_wrkr_1 ", role
        return _map_pid_wrkr_1
    elseif role == :worker 
    #    @info "Map_pid_wrkr_0 ", role
        return _map_pid_wrkr_0
    elseif role == :default && myrole() == :master
    #    @info "Map_pid_wrkr_1 ", role, pg.level
        return _map_pid_wrkr_1 # as :manager
    elseif role == :default && myrole() == :worker
    #    @info "Map_pid_wrkr_0 ", role, pg.level
        return _map_pid_wrkr_0 # as :worker
    else
        throw("unexpected use of role = :default (Map_pid_wrkr)")
   end    
end

# whether process is a master or worker in a distributed setup
myrole() = LPROCROLE[]
function myrole!(proctype::Symbol)
    LPROCROLE[] = proctype
end

# cluster management related API
"""
    myid()

Get the id of the current process.

# Examples
```julia-repl
julia> myid()
1

julia> remotecall_fetch(() -> myid(), 4)
4
```
"""
function myid(;role= :default) 
    if role == :manager 
        return LPROC.id1
    elseif role == :worker 
        return LPROC.id0
    elseif role == :default && myrole() == :master
        return LPROC.id1 # as :manager
    elseif role == :default && myrole() == :worker
        return LPROC.id0 # as :worker
    else
        throw("unexpected use of role := default (myid)")
    end

end

"""
    nprocs()

Get the number of available processes.

# Examples
```julia-repl
julia> nprocs()
3

julia> workers()
2-element Array{Int64,1}:
 2
 3
```
"""
function nprocs(; role= :default)
    pg = PGRP(role = role)
    if myid(role=role) == 1 || (pg.topology === :all_to_all && !isclusterlazy(role = role))
        n = length(pg.workers)
        # filter out workers in the process of being setup/shutdown.
        for jw in pg.workers
            if !isa(jw, LocalProcess) && (jw.state !== W_CONNECTED)
                n = n - 1
            end
        end
        return n
    else
        return length(pg.workers)
    end
end

"""
    nworkers()

Get the number of available worker processes. This is one less than [`nprocs()`](@ref). Equal to
`nprocs()` if `nprocs() == 1`.

# Examples
```julia-repl
\$ julia -p 2

julia> nprocs()
3

julia> nworkers()
2
```
"""
function nworkers(;role= :default)
    n = nprocs(role = role)
    n == 1 ? 1 : n-1
end

"""
    procs()

Return a list of all process identifiers, including pid 1 (which is not included by [`workers()`](@ref)).

# Examples
```julia-repl
\$ julia -p 2

julia> procs()
3-element Array{Int64,1}:
 1
 2
 3
```
"""
function procs(; role= :default)
    pg = PGRP(role = role)
    if myid(role=role) == 1 || (pg.topology === :all_to_all  && !isclusterlazy(role = role))
        # filter out workers in the process of being setup/shutdown.
        return Int[id(x, role=role) for x in pg.workers if isa(x, LocalProcess) || (x.state === W_CONNECTED)]
    else
        return Int[id(x, role=role) for x in pg.workers]
    end
end

function id_in_procs(id0; role= :default)  # faster version of `id in procs()`
    pg = PGRP(role = role)
    if myid(role=role) == 1 || (pg.topology === :all_to_all  && !isclusterlazy(role = role))
        for x in pg.workers
            if (id(x, role=role)::Int) == id0 && (isa(x, LocalProcess) || (x::Worker).state === W_CONNECTED)
                return true
            end
        end
    else
        for x in pg.workers
            if (id(idx, role=role)::Int) == id0
                return true
            end
        end
    end
    return false
end

"""
    procs(pid::Integer)

Return a list of all process identifiers on the same physical node.
Specifically all workers bound to the same ip-address as `pid` are returned.
"""
function procs(pid::Integer; role= :default)
    if myid(role = role) == 1
        map_pid_wrkr = Map_pid_wrkr(role = role)
        all_workers = [x for x in PGRP(role = role).workers if isa(x, LocalProcess) || (x.state === W_CONNECTED)]
        if (pid == 1) || (isa(map_pid_wrkr[pid].manager, LocalManager))
            Int[id(x, role=role) for x in filter(w -> (id(w, role=role)==1) || (isa(w.manager, LocalManager)), all_workers)]
        else
            ipatpid = get_bind_addr(pid)
            Int[id(x, role=role) for x in filter(w -> get_bind_addr(w) == ipatpid, all_workers)]
        end
    else
        remotecall_fetch(pid -> procs(pid, role = :manager), 1; role = role)
    end
end

"""
    workers()

Return a list of all worker process identifiers.

# Examples
```julia-repl
\$ julia -p 2
, pid
julia> workers()
2-element Array{Int64,1}:
 2
 3
```
"""
function workers(; role= :default)
    allp = procs(role = role)
    if length(allp) == 1
       allp
    else
       filter(x -> x != 1, allp)
    end
end

#function cluster_mgmt_from_master_check()
#    if myid() != 1
#        throw(ErrorException("Only process 1 can add and remove workers"))
#    end
#end

"""
    rmprocs(pids...; waitfor=typemax(Int))

Remove the specified workers. Note that only process 1 can add or remove
workers.

Argument `waitfor` specifies how long to wait for the workers to shut down:
  - If unspecified, `rmprocs` will wait until all requested `pids` are removed.
  - An [`ErrorException`](@ref) is raised if all workers cannot be terminated before
    the requested `waitfor` seconds.
  - With a `waitfor` value of 0, the call returns immediately with the workers
    scheduled for removal in a different task. The scheduled [`Task`](@ref) object is
    returned. The user should call [`wait`](@ref) on the task before invoking any other
    parallel calls.

# Examples
```julia-repl
\$ julia -p 5

julia> t = rmprocs(2, 3, waitfor=0)
Task (runnable) @0x0000000107c718d0

julia> wait(t)

julia> workers()
3-element Array{Int64,1}:
 4
 5
 6
```
"""
function rmprocs(pids...; waitfor=typemax(Int))    # supposed to be called always as :manager
#    cluster_mgmt_from_master_check()

    pids = vcat(pids...)
    if waitfor == 0
        t = @async _rmprocs(pids, typemax(Int))
        yield()
        return t
    else
        _rmprocs(pids, waitfor)
        # return a dummy task object that user code can wait on.
        return @async nothing
    end
end

function _rmprocs(pids, waitfor)
    lock(worker_lock)
    try
        rmprocset = Union{LocalProcess, Worker}[]
        for p in pids
            if p == 1
                @warn "rmprocs: process 1 not removed"
            else
                map_pid_wrkr = Map_pid_wrkr(role = :manager)
                if haskey(map_pid_wrkr, p)
                    w = map_pid_wrkr[p]
                    set_worker_state(w, W_TERMINATING)
                    kill(w.manager, p, w.config)
                    push!(rmprocset, w)
                end
            end
        end

        start = time_ns()
        while (time_ns() - start) < waitfor*1e9
            all(w -> w.state === W_TERMINATED, rmprocset) && break
            sleep(min(0.1, waitfor - (time_ns() - start)/1e9))
        end

        unremoved = [id(wrkr, role=role) for wrkr in filter(w -> w.state !== W_TERMINATED, rmprocset)]
        if length(unremoved) > 0
            estr = string("rmprocs: pids ", unremoved, " not terminated after ", waitfor, " seconds.")
            throw(ErrorException(estr))
        end
    finally
        unlock(worker_lock)
    end
end


"""
    ProcessExitedException(worker_id::Int)

After a client Julia process has exited, further attempts to reference the dead child will
throw this exception.
"""
struct ProcessExitedException <: Exception
    worker_id::Int
end

# No-arg constructor added for compatibility with Julia 1.0 & 1.1, should be deprecated in the future
ProcessExitedException() = ProcessExitedException(-1)

worker_from_id(i; role= :default) = worker_from_id(PGRP(role = role), i; role = role)
function worker_from_id(pg::ProcessGroup, i; role= :default)
    if !isempty(map_del_wrkr) && in(i, map_del_wrkr)
        throw(ProcessExitedException(i))
    end
    map_pid_wrkr = Map_pid_wrkr(role = role)
    w = get(map_pid_wrkr, i, nothing)
    if w === nothing
        if myid(role=role) == 1
            error("no process with id $i exists")
        end
        w = Worker(i; role = role)
        map_pid_wrkr[i] = w
    else
        w = w::Union{Worker, LocalProcess}
    end
    w
end

"""
    worker_id_from_socket(s) -> pid

A low-level API which, given a `IO` connection or a `Worker`,
returns the `pid` of the worker it is connected to.
This is useful when writing custom [`serialize`](@ref) methods for a type,
which optimizes the data written out depending on the receiving process id.
"""
function worker_id_from_socket(s; role= :default)
    w = get(map_sock_wrkr, s, nothing)
    if isa(w,Worker)
        if s === w.r_stream || s === w.w_stream
            return id(w, role=role)
        end
    end
    if isa(s,IOStream) && fd(s)==-1
        # serializing to a local buffer
        return myid(role=role)
    end
    return -1
end


register_worker(w; role= :default) = register_worker(PGRP(role = role), w; role = role)
function register_worker(pg, w; role= :default)
    push!(pg.workers, w)
    map_pid_wrkr = Map_pid_wrkr(role = role)
    map_pid_wrkr[id(w, role=role)] = w
end

function register_worker_streams(w)
    map_sock_wrkr[w.r_stream] = w
    map_sock_wrkr[w.w_stream] = w
end

deregister_worker(pid; role= :default) = deregister_worker(PGRP(role = role), pid)
function deregister_worker(pg, pid; role= :default)
    pg.workers = filter(x -> !(id(x, role=role) == pid), pg.workers)
    map_pid_wrkr = Map_pid_wrkr(role = role)
    w = pop!(map_pid_wrkr, pid, nothing)
    if isa(w, Worker)
        if isdefined(w, :r_stream)
            pop!(map_sock_wrkr, w.r_stream, nothing)
            if w.r_stream != w.w_stream
                pop!(map_sock_wrkr, w.w_stream, nothing)
            end
        end

        if myid(role=role) == 1 && role === :manager && isdefined(w, :config)
            # Notify the cluster manager of this workers death
            manage(w.manager, id(w, role=role), w.config, :deregister)
            if pg.topology !== :all_to_all || isclusterlazy(role = role)
                for rpid in workers(role=role)
                    try
                        remote_do(pid ->  deregister_worker(pid, role = rpid == 1 ? :manager : :worker), rpid; role = :worker)
                    catch
                    end
                end
            end
        end
    end
    push!(map_del_wrkr, pid)

    # delete this worker from our remote reference client sets
    ids = []
    tonotify = []
    lock(client_refs) do
        for (id, rv) in pg.refs
            if in(pid, rv.clientset)
                push!(ids, id)
            end
            if rv.waitingfor == pid
                push!(tonotify, (id, rv))
            end
        end
        for id in ids
            del_client(pg, id, pid)
        end

        # throw exception to tasks waiting for this pid
        for (id, rv) in tonotify
            close(rv.c, ProcessExitedException(pid))
            delete!(pg.refs, id)
        end
    end
    return
end


function interrupt(pid::Integer)  
    @assert myid(role = :manager) == 1
    map_pid_wrkr = Map_pid_wrkr(role = :manager)
    w = map_pid_wrkr[pid]
    if isa(w, Worker)
        manage(w.manager, id(w, role=:manager), w.config, :interrupt)
    end
    return
end

"""
    interrupt(pids::Integer...)

Interrupt the current executing task on the specified workers. This is equivalent to
pressing Ctrl-C on the local machine. If no arguments are given, all workers are interrupted.
"""
interrupt(pids::Integer...) = interrupt([pids...])

"""
    interrupt(pids::AbstractVector=workers())

Interrupt the current executing task on the specified workers. This is equivalent to
pressing Ctrl-C on the local machine. If no arguments are given, all workers are interrupted.
"""
function interrupt(pids::AbstractVector=workers(role = :manager))
    @assert myid(role = :manager) == 1
    @sync begin
        for pid in pids
            @async interrupt(pid)
        end
    end
end

wp_bind_addr(p::LocalProcess) = p.bind_addr
wp_bind_addr(p) = p.config.bind_addr

function check_same_host(pids; role= :default)
    if myid(role = role) != 1
        return remotecall_fetch(pids -> check_same_host(pids, role = :manager), 1, pids; role = role)
    else
        # We checkfirst if all test pids have been started using the local manager,
        # else we check for the same bind_to addr. This handles the special case
        # where the local ip address may change - as during a system sleep/awake
        map_pid_wrkr = Map_pid_wrkr(role = role)
        if all(p -> (p==1) || (isa(map_pid_wrkr[p].manager, LocalManager)), pids)
            return true
        else
            first_bind_addr = notnothing(wp_bind_addr(map_pid_wrkr[pids[1]]))
            return all(p -> notnothing(wp_bind_addr(map_pid_wrkr[p])) == first_bind_addr, pids[2:end])
        end
    end
end

function terminate_all_workers(;role= :default)
    myid(role = role) != 1 && return

    if nprocs(role = role) > 1
        try
            rmprocs(workers(role = role); waitfor=5.0)
        catch _ex
            @warn "Forcibly interrupting busy workers" exception=_ex
            # Might be computation bound, interrupt them and try again
            interrupt(workers(role = role))
            try
                rmprocs(workers(role = role); waitfor=5.0)
            catch _ex2
                @error "Unable to terminate all workers" exception=_ex2,catch_backtrace()
            end
        end
    end
end

# initialize the local proc network address / port
function init_bind_addr()
    opts = JLOptions()
    if opts.bindto != C_NULL
        bind_to = split(unsafe_string(opts.bindto), ":")
        bind_addr = string(parse(IPAddr, bind_to[1]))
        if length(bind_to) > 1
            bind_port = parse(Int,bind_to[2])
        else
            bind_port = 0
        end
    else
        bind_port = 0
        try
            bind_addr = string(getipaddr())
        catch
            # All networking is unavailable, initialize bind_addr to the loopback address
            # Will cause an exception to be raised only when used.
            bind_addr = "127.0.0.1"
        end
    end
    global LPROC
    LPROC.bind_addr = bind_addr
    LPROC.bind_port = UInt16(bind_port)
end

using Random: randstring

let inited = false
    # do initialization that's only needed when there is more than 1 processor
    global function init_multi()
        if !inited
            inited = true
            push!(Base.package_callbacks, _require_callback)
            atexit(terminate_all_workers)                           # TO CHECK (role argument ???)
            init_bind_addr()
            cluster_cookie(randstring(HDR_COOKIE_LEN))
        end
        return nothing
    end
end

function init_parallel()
    start_gc_msgs_task(role = :manager)   # TO CHECK
    start_gc_msgs_task(role = :worker)    # TO CHECK

    # start in "head node" mode, if worker, will override later.
    #global PGRP
    global LPROC
    LPROC.id0 = 0
    LPROC.id1 = 1
    @assert isempty(PGRP(role = :manager).workers)    # TO CHECK
    @assert isempty(PGRP(role = :worker).workers)     # TO CHECK
    register_worker(LPROC; role = :manager)           # TO CHECK
    register_worker(LPROC; role = :worker)            # TO CHECK
end

write_cookie(io::IO) = print(io.in, string(cluster_cookie(), "\n"))

function get_threads_spec(opts)
    if opts.nthreads > 0
        @assert opts.nthreadpools >= 1
        @assert opts.nthreads_per_pool != C_NULL
        thr = "$(unsafe_load(opts.nthreads_per_pool))"
        if opts.nthreadpools == 2
            thr = "$(thr),$(unsafe_load(opts.nthreads_per_pool, 2))"
        end
        `--threads=$(thr)`
    else
        ``
    end
end

function get_gcthreads_spec(opts)
    if opts.nmarkthreads > 0 || opts.nsweepthreads > 0
        `--gcthreads=$(opts.nmarkthreads),$(opts.nsweepthreads)`
    else
        ``
    end
end

# Starts workers specified by (-n|--procs) and --machine-file command line options
function process_opts(opts)
    # startup worker.
    # opts.startupfile, opts.load, etc should should not be processed for workers.
    if opts.worker == 1
        # does not return
        if opts.cookie != C_NULL
            start_worker(unsafe_string(opts.cookie))
        else
            start_worker()
        end
    end

    # Propagate --threads to workers
    threads = get_threads_spec(opts)
    # Propagate --gcthreads to workers
    gcthreads = get_gcthreads_spec(opts)

    exeflags = `$threads $gcthreads`

    # add processors
    if opts.nprocs > 0
        addprocs(opts.nprocs; exeflags=exeflags)
    end

    # load processes from machine file
    if opts.machine_file != C_NULL
        addprocs(load_machine_file(unsafe_string(opts.machine_file)); exeflags=exeflags)
    end
    return nothing
end


function load_machine_file(path::AbstractString)
    machines = []
    for line in split(read(path, String),'\n'; keepempty=false)
        s = split(line, '*'; keepempty=false)
        map!(strip, s, s)
        if length(s) > 1
            cnt = all(isdigit, s[1]) ? parse(Int,s[1]) : Symbol(s[1])
            push!(machines,(s[2], cnt))
        else
            push!(machines,line)
        end
    end
    return machines
end