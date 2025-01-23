//
// Created by gtoscano on 4/1/23.
//

#ifndef CBO_EVALUATION_BMP_GRPS_H
#define CBO_EVALUATION_BMP_GRPS_H

#include <unordered_map>
#include <vector>
#include <string>

class BmpGrps {
    std::unordered_map<int, std::vector<std::vector<int>>> bmp_grps;
    std::vector<int> bmps;

public:
    BmpGrps();
    void print();
    void load_file(std::string filename);
    std::vector<std::vector<int>> get(int load_src);
    std::vector<int> get_load_srcs();

    bool valid_load_src(int load_src);
    void prefer(const std::vector<int>& list);
    void select_only(const std::vector<int>& list);
    std::unordered_map<int, std::vector<std::vector<int>>> get();
};

#endif //CBO_EVALUATION_BMP_GRPS_H
