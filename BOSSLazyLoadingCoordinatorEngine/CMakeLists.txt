cmake_minimum_required(VERSION 3.10)
project(BOSSLazyLoadingCoordinatorEngine)

############################## Custom build modes ###############################

set(CMAKE_CXX_FLAGS_SANITIZE "-fno-omit-frame-pointer -fsanitize=address,signed-integer-overflow,null,alignment,bounds,function,return,vla-bound -O0 -g"
  CACHE STRING
  "Flags used by the C++ compiler during Sanitize builds."
  FORCE)
set(CMAKE_C_FLAGS_SANITIZE "-fno-omit-frame-pointer -fsanitize=address,signed-integer-overflow,null,alignment,bounds,function,return,vla-bound -O0 -g"
  CACHE STRING
  "Flags used by the C compiler during Sanitize builds."
  FORCE)
set(CMAKE_EXE_LINKER_FLAGS_SANITIZE
  ${CMAKE_EXE_LINKER_FLAGS_DEBUG} CACHE STRING
  "Flags used for linking binaries during Sanitize builds."
  FORCE)
set(CMAKE_SHARED_LINKER_FLAGS_SANITIZE
  ${CMAKE_SHARED_LINKER_FLAGS_DEBUG} CACHE STRING
  "Flags used by the shared libraries linker during Sanitize builds."
  FORCE)
mark_as_advanced(
  CMAKE_CXX_FLAGS_SANITIZE		  CMAKE_EXE_LINKER_FLAGS_SANITIZE
  CMAKE_C_FLAGS_SANITIZE		  CMAKE_SHARED_LINKER_FLAGS_SANITIZE
  )

set(CMAKE_BUILD_TYPE "${CMAKE_BUILD_TYPE}" CACHE STRING
  "Choose the type of build, options are: None Debug Release RelWithDebInfo MinSizeRel Sanitize."
  FORCE)

############################### External Projects ###############################

set(CMAKE_POSITION_INDEPENDENT_CODE TRUE) # make sure all dependent libraries are compiled with that option

if(MSVC)
    set(BOSS_LINK_LIBRARY_PREFIX ${CMAKE_LINK_LIBRARY_PREFIX})
    set(BOSS_LINK_LIBRARY_SUFFIX ${CMAKE_LINK_LIBRARY_SUFFIX})
else()
    set(BOSS_LINK_LIBRARY_PREFIX ${CMAKE_SHARED_LIBRARY_PREFIX})
    set(BOSS_LINK_LIBRARY_SUFFIX ${CMAKE_SHARED_LIBRARY_SUFFIX})
endif(MSVC)

include(ExternalProject)


set(RELATIVE_BOSS_DIR "../BOSSCoreLib")
get_filename_component(BOSS_DIR ${RELATIVE_BOSS_DIR} ABSOLUTE)

# Print the absolute path
message(STATUS "Absolute path: ${BOSS_DIR}")

include_directories(${BOSS_DIR}/include)
link_directories(${BOSS_DIR}/lib) 
    
  ExternalProject_Add(catch2
	URL https://github.com/catchorg/Catch2/archive/v2.13.6.tar.gz
  DOWNLOAD_DIR $ENV{HOME}/.cmake-downloads/${CMAKE_PROJECT_NAME}
	CMAKE_ARGS -DCMAKE_INSTALL_PREFIX=${BOSSLazyLoadingCoordinatorEngine_BINARY_DIR}/deps -DCATCH_BUILD_TESTING=NO -DBUILD_TESTING=NO -DCATCH_ENABLE_WERROR=NO -DCATCH_INSTALL_DOCS=NO -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE} -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}  -DCMAKE_C_COMPILER=${CMAKE_C_COMPILER} -DCMAKE_CXX_FLAGS=${CMAKE_CXX_FLAGS}
	)

      ExternalProject_Add(googlebenchmark
	URL "https://github.com/google/benchmark/archive/refs/tags/v1.8.3.tar.gz"
	DOWNLOAD_DIR $ENV{HOME}/.cmake-downloads/${CMAKE_PROJECT_NAME}
	CMAKE_ARGS -DCMAKE_INSTALL_PREFIX=${BOSSLazyLoadingCoordinatorEngine_BINARY_DIR}/deps -DBENCHMARK_DOWNLOAD_DEPENDENCIES=ON -DBENCHMARK_ENABLE_GTEST_TESTS=OFF $<IF:$<CONFIG:>,,-DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}> -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER} -DCMAKE_CXX_FLAGS=${CMAKE_CXX_FLAGS}
	BUILD_BYPRODUCTS ${BOSSLazyLoadingCoordinatorEngine_BINARY_DIR}/deps/lib/${CMAKE_SHARED_LIBRARY_PREFIX}benchmark${CMAKE_STATIC_LIBRARY_SUFFIX} ${BOSSLazyLoadingCoordinatorEngine_BINARY_DIR}/deps/lib/${CMAKE_SHARED_LIBRARY_PREFIX}benchmark_main${CMAKE_STATIC_LIBRARY_SUFFIX}
	)
      set_target_properties(googlebenchmark PROPERTIES EXCLUDE_FROM_ALL TRUE)


#################################### Targets ####################################

if(MSVC)
  # not making any difference on Windows
  # and helps Visual Studio to parse it correctly as a target
  set(LibraryType SHARED)
else()
  set(LibraryType MODULE)
endif(MSVC)

if(MSVC)
  set(pluginInstallDir bin)
else()
  set(pluginInstallDir lib)
endif(MSVC)

set(ImplementationFiles Source/BOSSLazyLoadingCoordinatorEngine.cpp)
set(TestFiles Tests/BOSSLazyLoadingCoordinatorTests.cpp)
set(BenchmarkFiles Benchmarks/Benchmarks.cpp)

#### Main Executable

add_library(BOSSLazyLoadingCoordinatorEngine MODULE ${ImplementationFiles})
set_property(TARGET BOSSLazyLoadingCoordinatorEngine PROPERTY CXX_STANDARD 20) ## the core is c++ 17 but the engines may want to use 20
target_include_directories(BOSSLazyLoadingCoordinatorEngine SYSTEM PUBLIC ${BOSSLazyLoadingCoordinatorEngine_BINARY_DIR}/deps/include ${BOSS_DIR}/include)
target_link_directories(BOSSLazyLoadingCoordinatorEngine PUBLIC ${BOSSLazyLoadingCoordinatorEngine_BINARY_DIR}/deps/lib ${BOSS_DIR}/lib)
target_link_libraries(BOSSLazyLoadingCoordinatorEngine ${BOSS_DIR}/lib/libBOSS.so)

#### Tests Executable

add_executable(LLCTests ${ImplementationFiles} ${TestFiles})
add_dependencies(LLCTests catch2)
set_property(TARGET LLCTests PROPERTY CXX_STANDARD 20)
target_include_directories(LLCTests SYSTEM PUBLIC ${BOSSLazyLoadingCoordinatorEngine_BINARY_DIR}/deps/include ${BOSS_DIR}/include)
target_link_directories(LLCTests PUBLIC ${BOSSLazyLoadingCoordinatorEngine_BINARY_DIR}/deps/lib ${BOSS_DIR}/lib)
target_link_libraries(LLCTests ${BOSS_DIR}/lib/libBOSS.so)
set(PUBLIC_HEADER_LIST
  ${CMAKE_CURRENT_SOURCE_DIR}/Source/BOSSLazyLoadingCoordinatorEngine.hpp;
  )

#### Benchmarks Executable

add_executable(Benchmarks Benchmarks/Benchmarks.cpp)
target_compile_options(Benchmarks PUBLIC -DBENCHMARK_STATIC_DEFINE)
set_target_properties(Benchmarks PROPERTIES CXX_STANDARD 20 CXX_STANDARD_REQUIRED ON CXX_EXTENSIONS OFF)
target_link_libraries(Benchmarks Threads::Threads)
if(WIN32)
  target_link_libraries(Benchmarks shlwapi.lib)
endif(WIN32)
add_dependencies(Benchmarks googlebenchmark)

target_include_directories(Benchmarks SYSTEM PUBLIC ${BOSSLazyLoadingCoordinatorEngine_BINARY_DIR}/deps/include ${BOSS_DIR}/include)
target_link_directories(Benchmarks PUBLIC ${BOSSLazyLoadingCoordinatorEngine_BINARY_DIR}/deps/lib ${BOSS_DIR}/lib)
target_link_libraries(Benchmarks ${BOSS_LINK_LIBRARY_PREFIX}BOSS${BOSS_LINK_LIBRARY_SUFFIX})
target_link_libraries(Benchmarks ${BOSSLazyLoadingCoordinatorEngine_BINARY_DIR}/deps/lib/${CMAKE_SHARED_LIBRARY_PREFIX}benchmark${CMAKE_STATIC_LIBRARY_SUFFIX})
target_link_libraries(Benchmarks ${BOSS_DIR}/lib/libBOSS.so)

#### VTune

option(ENABLE_PROFILING TRUE)
if(ENABLE_PROFILING)
  # set(VTune_DIR "" CACHE PATH "Where to look for VTune installation")
  message("Including profiling data collection and using VTune_DIR: ${VTune_DIR}")
  find_path(ITT_NOTIFY_INCLUDE_DIR "ittnotify.h" HINTS ${VTune_DIR}/include)
  target_compile_options(Benchmarks PUBLIC -DWITH_ITT_NOTIFY)
else()
  message("Not including profiling data collection")
  unset(ITT_NOTIFY_INCLUDE_DIR CACHE)
endif()

if(ITT_NOTIFY_INCLUDE_DIR)
    message("Found itt notify header in ${ITT_NOTIFY_INCLUDE_DIR}")
    target_include_directories(Benchmarks SYSTEM PUBLIC ${ITT_NOTIFY_INCLUDE_DIR})
    target_link_libraries(Benchmarks ${ITT_NOTIFY_INCLUDE_DIR}/../lib64/libittnotify${CMAKE_STATIC_LIBRARY_SUFFIX})
    if(NOT WIN32)
        target_link_libraries(Benchmarks dl)
    endif(NOT WIN32)
endif(ITT_NOTIFY_INCLUDE_DIR)

#### Tie up

set_target_properties(BOSSLazyLoadingCoordinatorEngine PROPERTIES INSTALL_RPATH_USE_LINK_PATH TRUE)
set_target_properties(BOSSLazyLoadingCoordinatorEngine PROPERTIES PUBLIC_HEADER ${PUBLIC_HEADER_LIST})
install(TARGETS BOSSLazyLoadingCoordinatorEngine LIBRARY DESTINATION lib)

set_target_properties(LLCTests PROPERTIES INSTALL_RPATH_USE_LINK_PATH TRUE)
install(TARGETS LLCTests RUNTIME DESTINATION bin)

set_target_properties(Benchmarks PROPERTIES INSTALL_RPATH_USE_LINK_PATH TRUE)
install(TARGETS Benchmarks RUNTIME DESTINATION bin)

set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)


