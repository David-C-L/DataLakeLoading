cmake_minimum_required(VERSION 3.10)
project(BOSSDictionaryEncoderEngine)

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

  ########################## Other Options and Library Dependencies ############################

  ExternalProject_Add(catch2
    URL https://github.com/catchorg/Catch2/archive/v2.13.6.tar.gz
    DOWNLOAD_DIR $ENV{HOME}/.cmake-downloads/${CMAKE_PROJECT_NAME}
    CMAKE_ARGS -DCMAKE_INSTALL_PREFIX=${BOSSDictionaryEncoderEngine_BINARY_DIR}/deps -DCATCH_BUILD_TESTING=NO -DBUILD_TESTING=NO -DCATCH_ENABLE_WERROR=NO -DCATCH_INSTALL_DOCS=NO -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE} -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}  -DCMAKE_C_COMPILER=${CMAKE_C_COMPILER} -DCMAKE_CXX_FLAGS=${CMAKE_CXX_FLAGS}
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

set(ImplementationFiles Source/BOSSDictionaryEncoderEngine.cpp)
set(TestFiles Tests/BOSSDictionaryEncoderTests.cpp)

add_library(BOSSDictionaryEncoderEngine MODULE ${ImplementationFiles})

add_executable(DETests ${ImplementationFiles} ${TestFiles})
add_dependencies(DETests catch2)

set_property(TARGET BOSSDictionaryEncoderEngine PROPERTY CXX_STANDARD 20) ## the core is c++ 17 but the engines may want to use 20
set_property(TARGET DETests PROPERTY CXX_STANDARD 20) ## the core is c++ 17 but the engines may want to use 20

target_include_directories(BOSSDictionaryEncoderEngine SYSTEM PUBLIC ${BOSSDictionaryEncoderEngine_BINARY_DIR}/deps/include  ${BOSS_DIR}/include)
target_include_directories(DETests SYSTEM PUBLIC ${BOSSDictionaryEncoderEngine_BINARY_DIR}/deps/include ${BOSS_DIR}/include)

target_link_directories(BOSSDictionaryEncoderEngine PUBLIC ${BOSSDictionaryEncoderEngine_BINARY_DIR}/deps/lib ${BOSS_DIR}/lib)
target_link_directories(DETests PUBLIC ${BOSSDictionaryEncoderEngine_BINARY_DIR}/deps/lib ${BOSS_DIR}/lib)

target_link_libraries(BOSSDictionaryEncoderEngine ${BOSS_DIR}/lib/libBOSS.so)
target_link_libraries(DETests ${BOSS_DIR}/lib/libBOSS.so)

set(PUBLIC_HEADER_LIST
  ${CMAKE_CURRENT_SOURCE_DIR}/Source/BOSSDictionaryEncoderEngine.hpp;
  )


list(APPEND AllTargets BOSSDictionaryEncoderEngine)

foreach(Target IN LISTS AllTargets)
  if(NOT WIN32)
    target_link_libraries(${Target} dl)
  endif(NOT WIN32)
    target_link_libraries(${Target} Threads::Threads)
    set_target_properties(${Target} PROPERTIES POSITION_INDEPENDENT_CODE ON)
    if(MSVC)
        target_compile_options(${Target} PUBLIC "/Zc:__cplusplus")
        target_compile_options(${Target} PUBLIC "/EHsc")
    endif()
    set_target_properties(${Target} PROPERTIES CXX_STANDARD 17 CXX_STANDARD_REQUIRED ON CXX_EXTENSIONS OFF)
    target_include_directories(${Target} SYSTEM PUBLIC ${BOSSDictionaryEncoderEngine_BINARY_DIR}/deps/include)
    target_include_directories(${Target} SYSTEM PUBLIC ${CMAKE_BINARY_DIR}/deps/include)
    target_link_libraries(${Target} ${BOSS_DIR}/lib/libBOSS.so)
    if(UNIX AND NOT APPLE)
        target_link_libraries(${Target} rt)
    endif()
endforeach()

set_target_properties(BOSSDictionaryEncoderEngine PROPERTIES INSTALL_RPATH_USE_LINK_PATH TRUE)
set_target_properties(BOSSDictionaryEncoderEngine PROPERTIES PUBLIC_HEADER ${PUBLIC_HEADER_LIST})
install(TARGETS BOSSDictionaryEncoderEngine LIBRARY DESTINATION lib)

set_target_properties(DETests PROPERTIES INSTALL_RPATH_USE_LINK_PATH TRUE)
install(TARGETS DETests RUNTIME DESTINATION bin)
