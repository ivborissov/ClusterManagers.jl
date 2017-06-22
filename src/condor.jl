# ClusterManager for HTCondor

export HTCManager, addprocs_htc

immutable HTCManager <: ClusterManager
    np::Integer
end

function condor_script(portnum::Integer, np::Integer, params::Dict)
    dir = params[:dir]
    exename = params[:exename]
    exeflags = params[:exeflags]
    home = ENV["JULIA_PKGDIR"]
    hostname = "$(gethostname())"
    jobname = "julia-$(getpid())"
    tdir = "$home\\.julia-htc"
#   run(`cmd.exe /c mkdir $tdir`)

    scriptf = open("$tdir\\$jobname.bat", "w")
    println(scriptf, "set HOMEDRIVE=$home")
    println(scriptf, "set HOMEPATH=$home")
#   println(scriptf, "set JULIA_NUM_THREADS=%NUMBER_OF_PROCESSORS%")
    println(scriptf, "set Path=C:\\Program Files (x86)\\Nmap")
    println(scriptf, "cd $home\\bin")
#   println(scriptf, "julia.exe $(Base.shell_escape(worker_arg)) | telnet $(Base.shell_escape(hostname)) $portnum")
    println(scriptf, "julia.exe $(Base.shell_escape(worker_arg)) | ncat $(Base.shell_escape(hostname)) $portnum")
    close(scriptf)

    subf = open("$tdir\\$jobname.sub", "w")
    println(subf, "executable = $tdir\\$jobname.bat")
    println(subf, "requirements = Machine != \"$hostname\"")
    println(subf, "universe = vanilla")
    println(subf, "should_transfer_files = yes")
    println(subf, "when_to_transfer_output = ON_EXIT")
    println(subf, "transfer_input_files = $tdir\\$jobname.bat")
    println(subf, "Notification = Error")
    for i = 1:np
        println(subf, "output = $tdir\\$jobname-$i.o")
        println(subf, "error = $tdir\\$jobname-$i.e")
	println(subf, "log = $tdir\\$jobname-$i.l")
        println(subf, "queue")
    end
    close(subf)

    "$tdir\\$jobname.sub"
end

function launch(manager::HTCManager, params::Dict, instances_arr::Array, c::Condition)
    try
        portnum = rand(8000:9000)
        server = listen(portnum)
        np = manager.np

        script = condor_script(portnum, np, params)
        out,proc = open(`condor_submit $script`)
        if !success(proc)
            println("batch queue not available (could not run condor_submit)")
            return
        end
        print(readline(out))
        print("Waiting for $np workers: ")

        for i=1:np
            conn = accept(server)
            config = WorkerConfig()

            config.io = conn

            push!(instances_arr, config)
            notify(c)
            print("$i ")
        end
        println(".")

   catch e
        println("Error launching condor")
        println(e)
   end
end

function manage(manager::HTCManager, id::Integer, config::WorkerConfig, op::Symbol)
    if op == :finalize
        if !isnull(config.io)
            close(get(config.io))
        end
#     elseif op == :interrupt
#         job = config[:job]
#         task = config[:task]
#         # this does not currently work
#         if !success(`qsig -s 2 -t $task $job`)
#             println("Error sending a Ctrl-C to julia worker $id (job: $job, task: $task)")
#         end
    end
end

addprocs_htc(np::Integer) = addprocs(HTCManager(np))
