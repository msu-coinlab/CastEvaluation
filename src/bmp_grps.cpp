//

#include "bmp_grps.h"

#include <algorithm>
#include <fstream>
#include <iostream>
#include <span>
#include <string>
#include <unordered_map>
#include <vector>

#include "json.hpp"

using json = nlohmann::json;


BmpGrps::BmpGrps() {
}

std::vector<std::vector<int>> BmpGrps::get(int load_src){
    return bmp_grps[load_src];
}

std::vector<int> BmpGrps::get_load_srcs() {
   auto keys = bmp_grps | std::views::keys;
   std::vector<int> ret;
    for (const auto& key : keys) {
        ret.push_back(key);
    }
    return ret;
}

bool BmpGrps::valid_load_src(int load_src) {
    if (bmp_grps.contains(load_src)) {
        return true;
    }
    return false;
}

void BmpGrps::load_file(std::string filename) {
    //auto filename = "/opt/opt4cast/csvs/bmp_grp.json";
    std::ifstream file(filename);

    // Read the entire file into a string
    std::string str;
    file >> str;

    // Parse the JSON string into a JSON object
    json grps = json::parse(str);

    for (auto& [key, value] : grps.items()) {
        bmps.push_back(std::stoi(key));
        bmp_grps[std::stoi(key)] = grps[key].get<std::vector<std::vector<int>>>();
    }
}

void BmpGrps::print() {

    for (auto& [key,data] : bmp_grps) {
        std::cout<<key<<": "<<data.size()<<" ";
        for (auto& row : data) {
            std::cout<<"[";
            for (auto& cell : row) {
                std::cout << cell << ",";
            }
            std::cout<<"]";
        }
        std::cout << std::endl;
    }
}

void BmpGrps::prefer(const std::vector<int>& to_keep) {
    for (auto& [key,data] : bmp_grps) {
        std::vector<std::vector<int>> copy;
        for (auto& row : data) {
            std::vector<int> filtered;
            // Check if the vector includes any of the elements in to_keep
            bool result = std::ranges::any_of(row, [&to_keep](int x) {
                return std::ranges::find(to_keep, x) != to_keep.end();
            });
            if( result ) {
                std::copy_if(row.begin(), row.end(), std::back_inserter(filtered), [&to_keep](int x) {
                    return std::find(to_keep.begin(), to_keep.end(), x) != to_keep.end();
                });
            }
            else {
                filtered = row;
            }

            if (!filtered.empty()) {
                copy.push_back(filtered);
            }
        }
        bmp_grps[key]=copy;
    }
}

void BmpGrps::select_only(const std::vector<int>& to_keep) {

    for (auto& [key,data] : bmp_grps) {
        std::vector<std::vector<int>> copy;
        for (auto& row : data) {
            std::vector<int> filtered;
            std::copy_if(row.begin(), row.end(), std::back_inserter(filtered), [&to_keep](int x) {
                return std::find(to_keep.begin(), to_keep.end(), x) != to_keep.end();
            });
            if (!filtered.empty()) {
                copy.push_back(filtered);
            }
        }
        bmp_grps[key]=copy;
    }
    /*
    
    for (auto& row : copy) {
        std::erase_if(row, [&to_remove](int x) {
            return std::ranges::find(to_remove, x) != to_remove.end();
        });
    }
    */
}

std::unordered_map<int, std::vector<std::vector<int>>> BmpGrps::get() {
    return bmp_grps;
}

