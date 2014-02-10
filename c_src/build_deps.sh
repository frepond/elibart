#!/bin/sh

# /bin/sh on Solaris is not a POSIX compatible shell, but /usr/bin/ksh is.
if [ `uname -s` = 'SunOS' -a "${POSIX_SHELL}" != "true" ]; then
    POSIX_SHELL="true"
    export POSIX_SHELL
    exec /usr/bin/ksh $0 $@
fi
unset POSIX_SHELL # clear it so if we invoke other scripts, they run as ksh as well

LIBART_REPO="https://github.com/frepond/libart.git"
LIBART_VSN="master"

set -e

if [ `basename $PWD` != "c_src" ]; then
    # originally "pushd c_src" of bash
    # but no need to use directory stack push here
    cd c_src
fi

BASEDIR="$PWD"

# detecting gmake and if exists use it
# if not use make
# (code from github.com/tuncer/re2/c_src/build_deps.sh
which scons 1>/dev/null 2>/dev/null && MAKE=scons

if [ -z $MAKE ]; then
    echo 'ERROR: missing "scons" command'

    exit -1
fi

# Changed "make" to $MAKE

case "$1" in
    clean)
        rm -rf libart
        ;;

    compile)
        if [ ! -d libart ]; then
            git $LIBART_REPO 
            (cd libart && git checkout $LIBART_VSN)
        fi
        
        (cd libart && $MAKE)
        ;;

    test)
        (cd libart && $MAKE)
        (cd libart && ./test_runner)
        ;;

    get-deps)
        if [ ! -d libart ]; then
            git clone $LIBART_REPO
            (cd libart && git checkout $LIBART_VSN)
        fi
        ;;

    *)
        if [ ! -d libart ]; then
            git clone $LIBART_REPO
            (cd libart && git checkout $LIBART_VSN)
        fi

        (cd libart && $MAKE)
        (cd libart && ./test_runner)
        ;;
esac
