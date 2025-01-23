//
// Created by gtoscano on 3/31/23.
//

#ifndef OTHER_REPORT_LOADS_H
#define OTHER_REPORT_LOADS_H
#include <vector>
#include <string>

struct ReportLoadSt {
    int lrseg;
    int agency;
    int load_src;
    int sector;
    double amount;
    int unit;
    double n_eos;
    double p_eos;
    double s_eos;
    double n_eor;
    double p_eor;
    double s_eor;
    double n_eot;
    double p_eot;
    double s_eot;
    bool is_valid;
};

class ReportLoads {
public:
    ReportLoads();
    ReportLoads(const ReportLoads& rhs);
    ReportLoads& operator=(const ReportLoads& rhs);

    void load(const std::string& filename);
    void print();
    std::vector<ReportLoadSt> get_parcels();
    std::vector<ReportLoadSt> get_valid_parcels();
    std::vector<ReportLoadSt> get_invalid_parcels();
    std::vector<ReportLoadSt> get_all_parcels();
    void remove_invalid();
    void remove_load_srcs(const std::vector<int>& to_compare);
    void keep_only_load_srcs(const std::vector<int>& to_compare);
    std::vector<double> compute_loads(std::string which);

    void print_loads();
private:
    std::vector<std::string> headings;
    std::vector<ReportLoadSt> parcels;
    std::vector<ReportLoadSt> parcels_valid_;
    std::vector<ReportLoadSt> parcels_invalid_;
    std::vector<ReportLoadSt> parcels_all_;
    double n_eos_sum, p_eos_sum, s_eos_sum;
    double n_eor_sum, p_eor_sum, s_eor_sum;
    double n_eot_sum, p_eot_sum, s_eot_sum;
};
#endif //OTHER_REPORT_LOADS_H
