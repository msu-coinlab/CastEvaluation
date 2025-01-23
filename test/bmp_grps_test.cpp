//
// Created by gtoscano on 4/1/23.
//

#include "bmp_grps.h"

#include <iostream>
#include <string>

int main() {
    BmpGrps lbg;

    auto filename = "/opt/opt4cast/csvs/land_conversion_bmp_grp2.json";
    //auto filename = "/opt/opt4cast/csvs/bmp_grp.json";
    lbg.load_file(filename);
    lbg.print();
    //lbg.select_only({9,11});
    /*
    lbg.select_only({29,3,145,82,46,137,142,139,48});
    std::cout<<"-----------------------\n";
    lbg.print();
    std::cout<<"-----------------------\n";
    auto a = lbg.get();
    */


    return 0;
}

