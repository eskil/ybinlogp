#!/bin/sh
AR="g++ -shared" \
LD="g++" \
LDFLAGS="-L/nail/home/eskil/local/lib -lboost_python -lpython2.5" \
CXXFLAGS="-fPIC -DBOOST_PYTHON_NO_PY_SIGNATURES -DDARWIN -I/nail/home/eskil/local/include -I/usr/include/python2.5" \
make $*
