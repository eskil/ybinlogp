#!/bin/sh
AR="g++ -shared" \
LD="g++" \
LDFLAGS="-L/opt/local/lib -lboost_python -lpython2.6" \
CXXFLAGS="-DBOOST_PYTHON_NO_PY_SIGNATURES -DDARWIN -I/opt/local/include -I/opt/local//include/python2.5" \
make $*