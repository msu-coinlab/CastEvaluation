# my_project/test/CMakeLists.txt
enable_testing()
# find Boost
find_package(Boost 1.65.1 COMPONENTS program_options REQUIRED)

add_executable(land_scenario_test land_scenario_test.cpp)
target_include_directories(land_scenario_test PUBLIC include gtest)
target_link_libraries(land_scenario_test PRIVATE land_scenario pthread arrow parquet gtest gtest_main Boost::program_options)

add_executable(bmp_grps_test bmp_grps_test.cpp)
target_include_directories(bmp_grps_test PUBLIC include)
target_link_libraries(bmp_grps_test PRIVATE land_scenario pthread arrow parquet )

add_executable(report_loads_test report_loads_test.cpp)
target_include_directories(report_loads_test PUBLIC include gtest)
target_link_libraries(report_loads_test PRIVATE report_loads pthread )

add_executable(simple_test simple_test.cpp)
target_include_directories(simple_test PUBLIC include gtest)
target_link_libraries(simple_test PRIVATE land_scenario gtest gtest_main)

add_executable(scenario_test scenario_test.cpp)
target_include_directories(scenario_test PUBLIC include)
target_link_libraries(scenario_test PRIVATE land_scenario)

include(GoogleTest)
gtest_discover_tests(simple_test)


