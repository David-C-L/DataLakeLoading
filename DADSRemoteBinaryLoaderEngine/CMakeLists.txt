cmake_minimum_required(VERSION 3.10)
project(DADSRemoteBinaryLoaderEngine)

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
    set(DADS_LINK_LIBRARY_PREFIX ${CMAKE_LINK_LIBRARY_PREFIX})
    set(DADS_LINK_LIBRARY_SUFFIX ${CMAKE_LINK_LIBRARY_SUFFIX})
else()
    set(DADS_LINK_LIBRARY_PREFIX ${CMAKE_SHARED_LIBRARY_PREFIX})
    set(DADS_LINK_LIBRARY_SUFFIX ${CMAKE_SHARED_LIBRARY_SUFFIX})
endif(MSVC)

include(ExternalProject)

# Core DADS
  if(NOT DEFINED DADS_SOURCE_REPOSITORY)
    if(NOT GITHUB_TOKEN)
      message( FATAL_ERROR "You need to pass -DGITHUB_TOKEN=... to pass a github token to fetch the dads release or -DDADS_SOURCE_REPOSITORY= for a non-github clone" )
    endif()
    set(DADS_SOURCE_REPOSITORY https://little-big-h:${GITHUB_TOKEN}@github.com/symbol-store/DADS.git)
  endif()
  if(NOT DEFINED DADS_SOURCE_BRANCH)
    # set(DADS_SOURCE_BRANCH temp-int8-serial-span)
    set(DADS_SOURCE_BRANCH temp_lazy_loading)
  endif()
  message("getting dads sources from ${DADS_SOURCE_REPOSITORY}, branch ${DADS_SOURCE_BRANCH}")
  ExternalProject_Add(DADS
      GIT_REPOSITORY ${DADS_SOURCE_REPOSITORY}
      GIT_TAG ${DADS_SOURCE_BRANCH}
      GIT_SHALLOW ON
		  CMAKE_ARGS -DCMAKE_VERBOSE_MAKEFILE=${CMAKE_VERBOSE_MAKEFILE} -DCMAKE_INSTALL_PREFIX=${DADSRemoteBinaryLoaderEngine_BINARY_DIR}/deps $<IF:$<CONFIG:>,,-DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}>
                 -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}  -DCMAKE_C_COMPILER=${CMAKE_C_COMPILER} -DCMAKE_CXX_FLAGS=${CMAKE_CXX_FLAGS}
      BUILD_BYPRODUCTS ${DADSRemoteBinaryLoaderEngine_BINARY_DIR}/deps/lib/${DADS_LINK_LIBRARY_PREFIX}DADS${DADS_LINK_LIBRARY_SUFFIX}
      )

    

########################## Other Options and Library Dependencies ############################

  # set(CURL_INCLUDE_DIR )
  # set(CURL_LIBRARIES )

  find_package(CURL REQUIRED)
  ExternalProject_Add(catch2
	URL https://github.com/catchorg/Catch2/archive/v2.13.6.tar.gz
  DOWNLOAD_DIR $ENV{HOME}/.cmake-downloads/${CMAKE_PROJECT_NAME}
	CMAKE_ARGS -DCMAKE_INSTALL_PREFIX=${DADSRemoteBinaryLoaderEngine_BINARY_DIR}/deps -DCATCH_BUILD_TESTING=NO -DBUILD_TESTING=NO -DCATCH_ENABLE_WERROR=NO -DCATCH_INSTALL_DOCS=NO -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE} -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}  -DCMAKE_C_COMPILER=${CMAKE_C_COMPILER} -DCMAKE_CXX_FLAGS=${CMAKE_CXX_FLAGS}
	)
      
set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)
  
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

set(ImplementationFiles Source/DADSRemoteBinaryLoaderEngine.cpp)
set(TestFiles Tests/DADSRemoteBinaryLoaderTests.cpp)

add_library(DADSRemoteBinaryLoaderEngine MODULE ${ImplementationFiles})

add_executable(RBLTests ${ImplementationFiles} ${TestFiles})
add_dependencies(RBLTests catch2)

set_property(TARGET DADSRemoteBinaryLoaderEngine PROPERTY CXX_STANDARD 20) ## the core is c++ 17 but the engines may want to use 20
set_property(TARGET RBLTests PROPERTY CXX_STANDARD 20) ## the core is c++ 17 but the engines may want to use 20

target_include_directories(DADSRemoteBinaryLoaderEngine SYSTEM PUBLIC ${DADSRemoteBinaryLoaderEngine_BINARY_DIR}/deps/include ${CMAKE_BINARY_DIR}/deps/include) # ${CURL_INCLUDE_DIR})
target_include_directories(RBLTests SYSTEM PUBLIC ${DADSRemoteBinaryLoaderEngine_BINARY_DIR}/deps/include) # ${CURL_INCLUDE_DIR})

target_link_directories(DADSRemoteBinaryLoaderEngine PUBLIC ${DADSRemoteBinaryLoaderEngine_BINARY_DIR}/deps/lib) # ${CURL_LIBRARIES})
target_link_directories(RBLTests PUBLIC ${DADSRemoteBinaryLoaderEngine_BINARY_DIR}/deps/lib) # ${CURL_LIBRARIES})
add_dependencies(DADSRemoteBinaryLoaderEngine DADS)
add_dependencies(RBLTests DADS)

include_directories(${CURL_INCLUDE_DIR})
target_link_libraries(DADSRemoteBinaryLoaderEngine ${CMAKE_BINARY_DIR}/deps/lib/${DADS_LINK_LIBRARY_PREFIX}DADS${DADS_LINK_LIBRARY_SUFFIX} curl Threads::Threads dl rt)
target_link_libraries(RBLTests curl Threads::Threads dl rt)

set(PUBLIC_HEADER_LIST
  ${CMAKE_CURRENT_SOURCE_DIR}/Source/DADSRemoteBinaryLoaderEngine.hpp;
  )

set_target_properties(DADSRemoteBinaryLoaderEngine PROPERTIES INSTALL_RPATH_USE_LINK_PATH TRUE)
set_target_properties(DADSRemoteBinaryLoaderEngine PROPERTIES POSITION_INDEPENDENT_CODE ON)
set_target_properties(DADSRemoteBinaryLoaderEngine PROPERTIES CXX_STANDARD 17 CXX_STANDARD_REQUIRED ON CXX_EXTENSIONS OFF)
set_target_properties(DADSRemoteBinaryLoaderEngine PROPERTIES PUBLIC_HEADER ${PUBLIC_HEADER_LIST})
install(TARGETS DADSRemoteBinaryLoaderEngine LIBRARY DESTINATION lib)

set_target_properties(RBLTests PROPERTIES INSTALL_RPATH_USE_LINK_PATH TRUE)
install(TARGETS RBLTests RUNTIME DESTINATION bin)
