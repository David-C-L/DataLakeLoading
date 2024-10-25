cmake_minimum_required(VERSION 3.10)
project(DADSVeloxEngine)

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
        CMAKE_CXX_FLAGS_SANITIZE CMAKE_EXE_LINKER_FLAGS_SANITIZE
        CMAKE_C_FLAGS_SANITIZE CMAKE_SHARED_LINKER_FLAGS_SANITIZE
)

set(CMAKE_BUILD_TYPE "${CMAKE_BUILD_TYPE}" CACHE STRING
        "Choose the type of build, options are: None Debug Release RelWithDebInfo MinSizeRel Sanitize."
        FORCE)

############################### External Projects ###############################
if (NOT VELOX_DIRS)
    message(FATAL_ERROR "You need to pre-build Velox Project and set VELOX_DIRS")
endif ()
set(CMAKE_MODULE_PATH ${VELOX_DIRS} ${CMAKE_MODULE_PATH})

set(CMAKE_POSITION_INDEPENDENT_CODE TRUE) # make sure all dependent libraries are compiled with that option

if (MSVC)
    set(DADS_LINK_LIBRARY_PREFIX ${CMAKE_LINK_LIBRARY_PREFIX})
    set(DADS_LINK_LIBRARY_SUFFIX ${CMAKE_LINK_LIBRARY_SUFFIX})
else ()
    set(DADS_LINK_LIBRARY_PREFIX ${CMAKE_SHARED_LIBRARY_PREFIX})
    set(DADS_LINK_LIBRARY_SUFFIX ${CMAKE_SHARED_LIBRARY_SUFFIX})
endif (MSVC)

include(ExternalProject)

set(RELATIVE_DADS_DIR "../DADSCoreLib")
get_filename_component(DADS_DIR ${RELATIVE_DADS_DIR} ABSOLUTE)

# Print the absolute path
message(STATUS "Absolute path: ${DADS_DIR}")

include_directories(${DADS_DIR}/include)
link_directories(${DADS_DIR}/lib) 

include_directories(${DADSVeloxEngine_BINARY_DIR}/deps/include)
link_directories(${DADSVeloxEngine_BINARY_DIR}/deps/lib)

#################################### VeloxEngine Plugin ####################################

set(ImplementationFiles Source/DADSVeloxEngine.cpp Source/BridgeVelox.cpp Source/DadsConnector.cpp
        ${VELOX_DIRS}/velox/exec/tests/utils/PlanBuilder.cpp
        ${VELOX_DIRS}/velox/exec/tests/utils/Cursor.cpp)

if (MSVC)
    # not making any difference on Windows
    # and helps Visual Studio to parse it correctly as a target
    set(LibraryType SHARED)
else ()
    set(LibraryType MODULE)
endif (MSVC)

add_library(DADSVeloxEngine ${LibraryType} ${ImplementationFiles})

target_include_directories(DADSVeloxEngine PUBLIC
        ${VELOX_DIRS}
        ${VELOX_DIRS}/velox/
        ${VELOX_DIRS}/velox/external/xxhash/
        ${VELOX_DIRS}/third_party/
        ${VELOX_DIRS}/third_party/xsimd/
        ${VELOX_DIRS}/third_party/xsimd/include/
	${DADS_DIR}/include)

set(VELOX_BUILD_TESTING OFF)
set(VELOX_CODEGEN_SUPPORT OFF)
set(VELOX_ENABLE_HIVE_CONNECTOR ON)
set(VELOX_ENABLE_TPCH_CONNECTOR ON)
set(VELOX_ENABLE_SPARK_FUNCTIONS OFF)

add_subdirectory(${VELOX_DIRS} velox)
target_link_libraries(
        DADSVeloxEngine
        velox_tpch_connector
        velox_hive_connector
        velox_dwio_dwrf_proto
        velox_aggregates
        velox_functions_prestosql
        velox_parse_parser
        atomic
	${DADS_DIR}/lib/libDADS.so
)
#copy from VLDB submission artifact
target_compile_options(DADSVeloxEngine PUBLIC -DARROW_STATIC -msse4.2 -DFOLLY_CFG_NO_COROUTINES -DFOLLY_HAS_COROUTINES=0)
if (UNIX AND NOT APPLE)
    target_link_libraries(DADSVeloxEngine uuid)
endif ()
if (NOT WIN32)
    target_link_libraries(DADSVeloxEngine dl)
endif (NOT WIN32)

set_property(TARGET DADSVeloxEngine PROPERTY CXX_STANDARD 20)
if (MSVC)
    target_compile_options(DADSVeloxEngine PUBLIC "/Zc:__cplusplus")
    target_compile_options(DADSVeloxEngine PUBLIC "/EHsc")
    # to link against all needed symbols for loading BulKDADS as a plugin
    target_link_libraries(${targetName} ${DADS_BINARY_DIR}/DADS.lib)
  endif (MSVC)
  
target_link_directories(DADSVeloxEngine PUBLIC ${DADSVeloxEngine_BINARY_DIR}/deps/lib ${DADS_DIR}/lib)
target_include_directories(DADSVeloxEngine SYSTEM PUBLIC ${DADSVeloxEngine_BINARY_DIR}/deps/include ${DADS_DIR}/include)

################################ ITT module interface ################################

set(VTune_DIR "" CACHE PATH "Where to look for VTune installation")
find_path(ITT_NOTIFY_INCLUDE_DIR "ittnotify.h" HINTS ${VTune_DIR}/include)

add_library(ITTNotifySupport INTERFACE)
if(ITT_NOTIFY_INCLUDE_DIR)
  message(VERBOSE "found itt notify header in ${ITT_NOTIFY_INCLUDE_DIR}")
  target_include_directories(ITTNotifySupport SYSTEM INTERFACE ${ITT_NOTIFY_INCLUDE_DIR})
  target_link_libraries(ITTNotifySupport INTERFACE ${ITT_NOTIFY_INCLUDE_DIR}/../lib64/libittnotify${CMAKE_STATIC_LIBRARY_SUFFIX})
  if(NOT WIN32)
    target_link_libraries(ITTNotifySupport dl)
  endif(NOT WIN32)
  if (NOT CMAKE_BUILD_TYPE STREQUAL "Release")
    target_compile_options(ITTNotifySupport INTERFACE -DWITH_ITT_NOTIFY)
  else()
    target_compile_options(ITTNotifySupport INTERFACE -DINTEL_NO_ITTNOTIFY_API)
  endif()
endif(ITT_NOTIFY_INCLUDE_DIR)

################################ Installation ################################

set_target_properties(DADSVeloxEngine PROPERTIES INSTALL_RPATH_USE_LINK_PATH TRUE)
install(TARGETS DADSVeloxEngine LIBRARY DESTINATION lib)
