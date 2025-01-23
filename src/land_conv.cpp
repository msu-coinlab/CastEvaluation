//
#include <iostream>
#include <vector>
#include <ranges>
#include <span>
#include <iomanip>
#include <sstream>
#include <iostream>
#include <fstream>
#include <unordered_map>
#include <ranges>
#include <algorithm>

#include "json.hpp"
#include "csv.hpp"

using json = nlohmann::json;

class LandBmpGrps{
    std::unordered_map<int, std::vector<std::vector<int>>> bmp_grps;
    std::vector<int> bmps;

    public:
        LandBmpGrps();
        void print();
        void load_file(std::string filename);
        void prefer(const std::vector<int>& list);
        void select_only(const std::vector<int>& list);
        std::unordered_map<int, std::vector<std::vector<int>>> get();
};


LandBmpGrps::LandBmpGrps() {
}    
void LandBmpGrps::load_file(std::string filename) {
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

void LandBmpGrps::print() {

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

void LandBmpGrps::prefer(const std::vector<int>& to_keep) {
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

void LandBmpGrps::select_only(const std::vector<int>& to_keep) {

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

std::unordered_map<int, std::vector<std::vector<int>>> LandBmpGrps::get() {
    return bmp_grps;
}

int main() {
    LandBmpGrps lbg;

    auto filename = "/opt/opt4cast/csvs/land_conversion_bmp_grp2.json";
    //auto filename = "/opt/opt4cast/csvs/bmp_grp.json";
    lbg.load_file(filename);
    lbg.print();
    lbg.select_only({9,11});
    std::cout<<"-----------------------\n";
    lbg.print();
    std::cout<<"-----------------------\n";
    auto a = lbg.get();


    // Print the vector of vectors of integers
    return 0;
}

