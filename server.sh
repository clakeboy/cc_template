#! /bin/sh
#程序启动脚本
### BEGIN INIT INFO
# Provides:          cc_template
# Required-Start:    $remote_fs $network
# Required-Stop:     $remote_fs $network
# Default-Start:     2 3 4 5
# Default-Stop:      0 1 6
# Short-Description: starts cc_template
# Description:       starts the cc_template generate service
### END INIT INFO
prefix=$(pwd)
exec_prefix=${prefix}/cc_template
pid_file=${prefix}/cc_template.pid
conf_file=${prefix}/main.conf

wait_for_pid () {
    try=0

    while test $try -lt 35 ; do

        case "$1" in
            'created')
            if [ -f "$2" ] ; then
                try=''
                break
            fi
            ;;

            'removed')
            if [ ! -f "$2" ] ; then
                try=''
                break
            fi
            ;;
        esac

        printf "."
        try=$((try + 1))
        sleep 1

    done

}

start_server() {
    if [ -r "$pid_file" ] ; then
        echo "cc_template is running"
        exit 1
    fi

    printf "Starting cc_template "

    opts=$($exec_prefix --pprof --cross --config "$conf_file" --pid "$pid_file" &)

    if ! $opts ; then
        printf " failed \n"
        exit 1
    fi

    wait_for_pid created "$pid_file"

    if [ -n "$try" ] ; then
        printf " failed \n"
        exit 1
    else
        printf " done \n"
    fi
}

stop_server() {
    printf "Gracefully shutting down cc_template "

    if [ ! -r "$pid_file" ] ; then
        printf "warning, no pid file found - cc_template is not running ? \n"
        exit 1
    fi

    kill -QUIT "$pid_file"

    wait_for_pid removed "$pid_file"

    if [ -n "$try" ] ; then
        printf " failed. Use force-quit \n"
        exit 1
    else
        printf " done \n"
    fi;
}

start_nohup_server() {
    printf "Starting nohup mode cc_template "

    opts=$(nohup "$exec_prefix" --pid "$pid_file" --pprof --cross --config "$conf_file" >> ./out.log 2>&1 &)

    if ! $opts ; then
        printf " failed \n"
        exit 1
    fi

    wait_for_pid created "$pid_file"

    if [ -n "$try" ] ; then
        printf " failed \n"
        exit 1
    else
        printf " done \n"
    fi
}

case "$1" in
    start)
        start_server
    ;;

    stop)
        stop_server
    ;;

    restart)
        stop_server
        start_nohup_server
    ;;

    nohup)
        start_nohup_server
    ;;
esac