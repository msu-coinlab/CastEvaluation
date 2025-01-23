cmake_minimum_required(VERSION 3.8)

project(cbo_evaluation)
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
include(FetchContent)

find_package(crossguid CONFIG REQUIRED)
find_package(Threads REQUIRED)
#cmake -DCMAKE_BUILD_TYPE=Debug

FetchContent_Declare(
        googletest
        GIT_REPOSITORY https://github.com/google/googletest.git
        GIT_TAG        release-1.11.0
)

FetchContent_MakeAvailable(googletest)

add_subdirectory(test)
# Add source files for library 1
set(LIB_LAND_SCENARIO_SOURCES
        src/amqp.cpp
        src/base_scenario.cpp
        src/bmp_grps.cpp
        src/data_reader.cpp
        src/land_scenario.cpp
        src/animal_scenario.cpp
        src/manure_scenario.cpp
        src/misc_utilities.cpp
        src/report_loads.cpp
        src/scenario.cpp
)

# Add header files for library 1
set(LIB_LAND_SCENARIO_HEADERS
        include/amqp.h
        include/base_scenario.h
        include/bmp_grps.h
        include/csv.hpp
        include/json.hpp
        include/data_reader.h
        include/land_scenario.h
        include/animal_scenario.h
        include/manure_scenario.h
        include/misc_utilities.h
        include/report_loads.h
        include/scenario.h
)

add_library(land_scenario ${LIB_LAND_SCENARIO_SOURCES} ${LIB_LAND_SCENARIO_HEADERS})
# Set include directories for library 1
target_include_directories(land_scenario PUBLIC include)
target_compile_features(land_scenario PUBLIC cxx_std_20)
target_link_libraries(land_scenario PRIVATE fmt pthread arrow parquet crossguid hiredis redis++ SimpleAmqpClient Threads::Threads)

set(LIB_REPORT_LOADS_SOURCES
        src/report_loads.cpp
        )

# Add header files for library 1
set(LIB_REPORT_LOADS_HEADERS
        include/json.hpp
        include/csv.hpp
        )

add_library(report_loads ${LIB_REPORT_LOADS_SOURCES} ${LIB_REPORT_LOADS_HEADERS})

# Set include directories for library 1
target_include_directories(report_loads PUBLIC include)
target_compile_features(report_loads PUBLIC cxx_std_20)
target_link_libraries(report_loads PRIVATE fmt pthread)
