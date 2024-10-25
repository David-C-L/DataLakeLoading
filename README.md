# On Demand Lake Data Loading (Load-Evaluate Architecture)

## Building

Each folder contains an engine that can be built as a dynamic library by running the following commands:

- mkdir build
- cmake -DCMAKE_C_COMPILER=clang-14 -DCMAKE_CXX_COMPILER=clang++-14 -DCMAKE_BUILD_TYPE=RelWithDebInfo -B. ..
- cmake --build .

The exception is the VeloxEngine. This engine depends on Velox, which can bloat your folders and be difficult to build, so the dynamics library for the VeloxEngine (libDADSVeloxEngine.so) has been provided via git lfs at "./DADSVeloxEngine/premade_lib/libDADSVeloxEngine.so".

## Benchmarks

The benchmarks and corresponding queries are located in "DADSLazyLoadingCoordinatorEngine/Benchmarks/".

Before starting any benchmarking, the "config.hpp" file in this folder must be filled out with the paths to the dynamics libraries that you have built. This can be done by replacing "/enter/path/to/" with the absolute path to the appropriate library.

Also in this folder, a python script can also be found. Running the following command from the "Benchmarks" folder should run all possible benchmarks and produce the corresponding plots:

- python run_benchmarks.py "/path/to/DADSLazyLoadingCoordinatorEngine/build/dir/"

Notably, the benchmarks requiring VTune will not run as they require manual counting to produce the plots.

## Results

If all else fails, this repository also contains the data used to produce the graphs that make up the evaluation of this lazy loading system.
