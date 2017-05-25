import subprocess
import sys
import os

# check if pip is installed. If not, raise an ImportError
PIP_INSTALLED = True

try:
    import pip
except ImportError:
    PIP_INSTALLED = False

if not PIP_INSTALLED:
    raise ImportError('pip is not installed.')

def install_and_import(package):
    import importlib
    try:
        importlib.import_module(package)
    except ImportError:
        pip.main(['install', package])
    finally:
        globals()[package] = importlib.import_module(package)

# check if setuptools is installed. If not, install setuptools
# automatically using pip.
install_and_import('setuptools')

from setuptools.command.build_ext import build_ext as _build_ext

class build_ext(_build_ext):
    def build_extensions(self):
        _build_ext.build_extensions(self)

def generate_cython():
    cwd = os.path.abspath(os.path.dirname(__file__))
    print("Cythonizing sources")
    p = subprocess.call([sys.executable, os.path.join(cwd,
                                                      'build_tools',
                                                      'cythonize.py'),
                         'benchmarks'],
                        cwd=cwd)
    if p != 0:
        raise RuntimeError("Running cythonize failed!")


cmdclass = {"build_ext": build_ext}


if __name__ == "__main__":

    no_frills = (len(sys.argv) >= 2 and ('--help' in sys.argv[1:] or
                                         sys.argv[1] in ('--help-commands',
                                                         'egg_info', '--version',
                                                         'clean')))

    cwd = os.path.abspath(os.path.dirname(__file__))
    if not os.path.exists(os.path.join(cwd, 'PKG-INFO')) and not no_frills:
        # Generate Cython sources, unless building from source release
        generate_cython()

    # specify extensions that need to be compiled
    extensions = [

            setuptools.Extension("dmagellan.utils.cy_utils.stringcontainer", sources=["dmagellan/utils/cy_utils/stringcontainer.pyx"], language="c++", extra_compile_args = ["-O3", "-ffast-math", "-march=native", "-fopenmp"], extra_link_args=['-fopenmp']),

            setuptools.Extension("dmagellan.utils.cy_utils.tokencontainer", sources=["dmagellan/utils/cy_utils/tokencontainer.pyx"], language="c++", extra_compile_args = ["-O3", "-ffast-math", "-march=native", "-fopenmp"], extra_link_args=['-fopenmp']),

            setuptools.Extension("dmagellan.utils.cy_utils.invertedindex", sources=["dmagellan/utils/cy_utils/invertedindex.pyx"], language="c++", extra_compile_args = ["-O3", "-ffast-math", "-march=native", "-fopenmp"], extra_link_args=['-fopenmp']),


            setuptools.Extension("dmagellan.tokenizer.tokenizer", sources=["dmagellan/tokenizer/tokenizer.pyx"], language="c++", extra_compile_args = ["-O3", "-ffast-math", "-march=native", "-fopenmp"], extra_link_args=['-fopenmp']),

            setuptools.Extension("dmagellan.tokenizer.whitespacetokenizer", sources=["dmagellan/tokenizer/whitespacetokenizer.pyx"], language="c++", extra_compile_args = ["-O3", "-ffast-math", "-march=native", "-fopenmp"], extra_link_args=['-fopenmp']),

            setuptools.Extension("dmagellan.tokenizer.qgramtokenizer", sources=["dmagellan/tokenizer/qgramtokenizer.pyx"], language="c++", extra_compile_args = ["-O3", "-ffast-math", "-march=native", "-fopenmp"], extra_link_args=['-fopenmp']),

            setuptools.Extension("dmagellan.sampler.downsample.dsprober", sources=["dmagellan/sampler/downsample/dsprober.pyx"], language="c++", extra_compile_args = ["-O3", "-ffast-math", "-march=native", "-fopenmp"], extra_link_args=['-fopenmp']),

            setuptools.Extension("dmagellan.blocker.overlap.overlapprober", sources=["dmagellan/blocker/overlap/overlapprober.pyx"], language="c++", extra_compile_args = ["-O3", "-ffast-math", "-march=native", "-fopenmp"], extra_link_args=['-fopenmp']),


            # setuptools.Extension("dmagellan.core.utils", sources=["dmagellan/core/utils.pyx"], language="c++",
            #           extra_compile_args = ["-O3", "-ffast-math", "-march=native", "-fopenmp"],
            #                         extra_link_args=['-fopenmp']

    ]







    # find packages to be included. exclude benchmarks.
    packages = setuptools.find_packages()

    with open('README.md') as f:
        LONG_DESCRIPTION = f.read()

    setuptools.setup(
        name='dmagellan',
        version='0.1.0',
        description='EM-Dask',
        long_description=LONG_DESCRIPTION,
        author='UW Magellan Team',
        author_email='uwmagellan@gmail.com',
        license='BSD',
        classifiers=[
            'Development Status :: 4 - Beta',
            'Environment :: Console',
            'Intended Audience :: Developers',
            'Intended Audience :: Science/Research',
            'Intended Audience :: Education',
            'License :: OSI Approved :: BSD License',
            'Operating System :: POSIX',
            'Operating System :: Unix',
            'Operating System :: MacOS',
            'Operating System :: Microsoft :: Windows',
            'Programming Language :: Python',
            'Programming Language :: Python :: 2',
            'Programming Language :: Python :: 3',
            'Programming Language :: Python :: 2.7',
            'Programming Language :: Python :: 3.3',
            'Programming Language :: Python :: 3.4',
            'Programming Language :: Python :: 3.5',
            'Topic :: Scientific/Engineering',
            'Topic :: Utilities',
            'Topic :: Software Development :: Libraries',
        ],
     	install_requires=[
            'PyPrind == 2.9.8',
	    'py_entitymatching',
            'py_stringsimjoin',
            # dependencies such as py_stringmatching, joblib, pyprind
            'cloudpickle >= 0.2.1',
            'pyparsing >= 2.1.4',
            'scikit-learn >= 0.18',
            'dask',
            'distributed'
        ],
        packages=packages,
        ext_modules=extensions,
        cmdclass=cmdclass,
        include_package_data=True,
        zip_safe=False
    )
