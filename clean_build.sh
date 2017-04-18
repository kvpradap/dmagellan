#!/usr/bin/env bash
echo 'Cleaning generated files (*.so, *.cpp)'
find . -name \*.so -delete
find . -name \*.cpp -delete


echo 
echo 'Building ..'
rm -f .err
python setup.py build_ext --inplace 2> .err
#python setup.py build_ext --inplace --cython-gdb 2>.err

echo; echo
echo "Num. errors:`cat .err|grep -i error|wc -l`"

