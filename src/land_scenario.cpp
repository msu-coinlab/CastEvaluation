//
// Created by Gregorio Toscano on 3/27/23.
//
//True Forest!
// g++ land_scenario.cpp -std=c++20 -lfmt -lpthread -larrow -lparquet
#include "land_scenario.h"
#include "data_reader.h"

#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>
#include <parquet/exception.h>
#include <parquet/stream_writer.h>

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/csv/api.h>
#include <arrow/csv/writer.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/table.h>
#include <arrow/compute/api_aggregate.h>
#include <arrow/api.h>

#include <fmt/core.h>

LandScenario::LandScenario(const DataReader& data_reader) : data_(data_reader) {
}

/**
 * @brief Writes variable land_scenario into output_path, using exec_uuid
 *
 * @param[in] output_path The path for output files
 * @param[in] exec_uuid The text ID to use for output files
 *
 * @return the number of rows written
 *
 * @note This function does not perform any error checking or validation.
 */

int LandScenario::write(
        const std::string& output_path,
        const std::string& exec_uuid
) {
/*
    auto ret = write(
        land_scenario,
        output_path,
        exec_uuid
    );
    */
    int ret = 5;
    return ret;

}

/**
 * @brief Writes variable land_scenario into output_path, using exec_uuid
 *
 * @param[in] rows The rows
 * @param[in] output_path The path for output files
 * @param[in] exec_uuid The text ID to use for output files
 *
 * @return the number of rows written
 *
 * @note This function does not perform any error checking or validation.
 */
int LandScenario::write(
        const std::vector<ScenarioStruct>& rows,
        const std::string& output_path,
        const std::string& exec_uuid
) {
    std::shared_ptr<arrow::Schema> schema = arrow::schema ({
          arrow::field("BmpSubmittedId", arrow::int32()),
          arrow::field("AgencyId", arrow::int32()),
          arrow::field("StateUniqueIdentifier", arrow::utf8()), //it can be binary
          arrow::field("StateId", arrow::int32()),
          arrow::field("BmpId", arrow::int32()),
          arrow::field("GeographyId", arrow::int32()),
          arrow::field("LoadSourceGroupId", arrow::int32()),
          arrow::field("UnitId", arrow::int32()),
          arrow::field("Amount", arrow::float64()),
          arrow::field("IsValid", arrow::boolean()),
          arrow::field("ErrorMessage", arrow::utf8()),//it can be binary
          arrow::field("RowIndex", arrow::int32()),
          });
    std::unordered_map<int, double> bmp_sum;
    arrow::Int32Builder bmp_submitted_id;
    arrow::Int32Builder agency_id;
    arrow::StringBuilder state_unique_identifier;
    arrow::Int32Builder state_id;
    arrow::Int32Builder bmp_id;
    arrow::Int32Builder geography_id;
    arrow::Int32Builder load_source_id;
    arrow::Int32Builder unit_id_builder;
    arrow::DoubleBuilder amount_builder;
    arrow::BooleanBuilder is_valid;
    arrow::StringBuilder error_message;
    arrow::Int32Builder row_index;


    std::shared_ptr<arrow::io::FileOutputStream> outfile;

    std::string file_name_src = fmt::format
            ("{}/{}_impbmpsubmittedland_lc.parquet", output_path, exec_uuid);

    PARQUET_ASSIGN_OR_THROW(
            outfile,
            arrow::io::FileOutputStream::Open(file_name_src));

    parquet::WriterProperties::Builder builder;
    //builder.compression(parquet::Compression::ZSTD);
    builder.version(parquet::ParquetVersion::PARQUET_1_0);

    std::shared_ptr<parquet::schema::GroupNode> my_schema;

    parquet::schema::NodeVector fields;


    fields.push_back(parquet::schema::PrimitiveNode::Make(
            "BmpSubmittedId", parquet::Repetition::REQUIRED, parquet::Type::INT32, parquet::ConvertedType::INT_32
    ));
    fields.push_back(parquet::schema::PrimitiveNode::Make(
            "AgencyId", parquet::Repetition::REQUIRED, parquet::Type::INT32, parquet::ConvertedType::INT_32
    ));
    fields.push_back(parquet::schema::PrimitiveNode::Make(
            "StateUniqueIdentifier", parquet::Repetition::REQUIRED, parquet::Type::BYTE_ARRAY, parquet::ConvertedType::UTF8
    ));
    fields.push_back(parquet::schema::PrimitiveNode::Make(
            "StateId", parquet::Repetition::REQUIRED, parquet::Type::INT32, parquet::ConvertedType::INT_32
    ));
    fields.push_back(parquet::schema::PrimitiveNode::Make(
            "BmpId", parquet::Repetition::REQUIRED, parquet::Type::INT32, parquet::ConvertedType::INT_32
    ));
    fields.push_back(parquet::schema::PrimitiveNode::Make(
            "GeographyId", parquet::Repetition::REQUIRED, parquet::Type::INT32, parquet::ConvertedType::INT_32
    ));
    fields.push_back(parquet::schema::PrimitiveNode::Make(
            "LoadSourceGroupId", parquet::Repetition::REQUIRED, parquet::Type::INT32, parquet::ConvertedType::INT_32
    ));
    fields.push_back(parquet::schema::PrimitiveNode::Make(
            "UnitId", parquet::Repetition::REQUIRED, parquet::Type::INT32, parquet::ConvertedType::INT_32
    ));
    fields.push_back(parquet::schema::PrimitiveNode::Make(
            "Amount", parquet::Repetition::REQUIRED, parquet::Type::DOUBLE
    ));
    fields.push_back(parquet::schema::PrimitiveNode::Make(
            "IsValid", parquet::Repetition::REQUIRED, parquet::Type::BOOLEAN
    ));
    fields.push_back(parquet::schema::PrimitiveNode::Make(
            "ErrorMessage", parquet::Repetition::REQUIRED, parquet::Type::BYTE_ARRAY, parquet::ConvertedType::UTF8
    ));
    fields.push_back(parquet::schema::PrimitiveNode::Make(
            "RowIndex", parquet::Repetition::REQUIRED, parquet::Type::INT32, parquet::ConvertedType::INT_32
    ));

    my_schema = std::static_pointer_cast<parquet::schema::GroupNode>(
            parquet::schema::GroupNode::Make("schema", parquet::Repetition::REQUIRED, fields));

    parquet::StreamWriter os{
            parquet::ParquetFileWriter::Open(outfile, my_schema, builder.build())};


    int idx = 0;
    int counter = 0;
    for (auto const&row: rows) {
        os<<counter+1<<row.agency<<fmt::format("SU{}",counter)<<row.state<<row.bmp<<row.geo<<row.load_src_grp<<row.unit<<row.amount<<true<<""<<counter+1<<parquet::EndRow;
        counter++;
    }

    return counter;
}

int LandScenario::write_land(
        const std::vector<LandSol>& rows,
        const std::string& out_filename
) {
    std::shared_ptr<arrow::Schema> schema = arrow::schema ({
                                                                   arrow::field("BmpSubmittedId", arrow::int32()),
                                                                   arrow::field("AgencyId", arrow::int32()),
                                                                   arrow::field("StateUniqueIdentifier", arrow::utf8()), //it can be binary
                                                                   arrow::field("StateId", arrow::int32()),
                                                                   arrow::field("BmpId", arrow::int32()),
                                                                   arrow::field("GeographyId", arrow::int32()),
                                                                   arrow::field("LoadSourceGroupId", arrow::int32()),
                                                                   arrow::field("UnitId", arrow::int32()),
                                                                   arrow::field("Amount", arrow::float64()),
                                                                   arrow::field("IsValid", arrow::boolean()),
                                                                   arrow::field("ErrorMessage", arrow::utf8()),//it can be binary
                                                                   arrow::field("RowIndex", arrow::int32()),
                                                           });
    std::unordered_map<int, double> bmp_sum;
    arrow::Int32Builder bmp_submitted_id, agency_id;
    arrow::StringBuilder state_unique_identifier;
    arrow::Int32Builder state_id, bmp_id, geography_id, load_source_id, unit_id_builder;
    arrow::DoubleBuilder amount_builder;
    arrow::BooleanBuilder is_valid;
    arrow::StringBuilder error_message;
    arrow::Int32Builder row_index;


    std::shared_ptr<arrow::io::FileOutputStream> outfile;


    PARQUET_ASSIGN_OR_THROW(
            outfile,
            arrow::io::FileOutputStream::Open(out_filename));

    parquet::WriterProperties::Builder builder;
    //builder.compression(parquet::Compression::ZSTD);
    builder.version(parquet::ParquetVersion::PARQUET_1_0);

    std::shared_ptr<parquet::schema::GroupNode> my_schema;

    parquet::schema::NodeVector fields;


    fields.push_back(parquet::schema::PrimitiveNode::Make(
            "BmpSubmittedId", parquet::Repetition::REQUIRED, parquet::Type::INT32, parquet::ConvertedType::INT_32
    ));
    fields.push_back(parquet::schema::PrimitiveNode::Make(
            "AgencyId", parquet::Repetition::REQUIRED, parquet::Type::INT32, parquet::ConvertedType::INT_32
    ));
    fields.push_back(parquet::schema::PrimitiveNode::Make(
            "StateUniqueIdentifier", parquet::Repetition::REQUIRED, parquet::Type::BYTE_ARRAY, parquet::ConvertedType::UTF8
    ));
    fields.push_back(parquet::schema::PrimitiveNode::Make(
            "StateId", parquet::Repetition::REQUIRED, parquet::Type::INT32, parquet::ConvertedType::INT_32
    ));
    fields.push_back(parquet::schema::PrimitiveNode::Make(
            "BmpId", parquet::Repetition::REQUIRED, parquet::Type::INT32, parquet::ConvertedType::INT_32
    ));
    fields.push_back(parquet::schema::PrimitiveNode::Make(
            "GeographyId", parquet::Repetition::REQUIRED, parquet::Type::INT32, parquet::ConvertedType::INT_32
    ));
    fields.push_back(parquet::schema::PrimitiveNode::Make(
            "LoadSourceGroupId", parquet::Repetition::REQUIRED, parquet::Type::INT32, parquet::ConvertedType::INT_32
    ));
    fields.push_back(parquet::schema::PrimitiveNode::Make(
            "UnitId", parquet::Repetition::REQUIRED, parquet::Type::INT32, parquet::ConvertedType::INT_32
    ));
    fields.push_back(parquet::schema::PrimitiveNode::Make(
            "Amount", parquet::Repetition::REQUIRED, parquet::Type::DOUBLE
    ));
    fields.push_back(parquet::schema::PrimitiveNode::Make(
            "IsValid", parquet::Repetition::REQUIRED, parquet::Type::BOOLEAN
    ));
    fields.push_back(parquet::schema::PrimitiveNode::Make(
            "ErrorMessage", parquet::Repetition::REQUIRED, parquet::Type::BYTE_ARRAY, parquet::ConvertedType::UTF8
    ));
    fields.push_back(parquet::schema::PrimitiveNode::Make(
            "RowIndex", parquet::Repetition::REQUIRED, parquet::Type::INT32, parquet::ConvertedType::INT_32
    ));

    my_schema = std::static_pointer_cast<parquet::schema::GroupNode>(
            parquet::schema::GroupNode::Make("schema", parquet::Repetition::REQUIRED, fields));

    parquet::StreamWriter os{
            parquet::ParquetFileWriter::Open(outfile, my_schema, builder.build())};


    int idx = 0;
    int counter = 0;
    for (auto const&row: rows) {
        int state = data_.get_state(row.lrseg);
        int geography = data_.get_geography(row.lrseg);
        int load_src_grp = data_.get_u_u_group(row.load_src);
        int unit = 1; //acres
        os<<counter+1<<row.agency<<fmt::format("SU{}",counter)<<state<<row.bmp<<geography<<load_src_grp<<unit<<row.amount<<true<<""<<counter+1<<parquet::EndRow;
        counter++;
    }

    return counter;
}

/**
 * @brief Loads a land scenario
 *
 * This function takes an string which is the key for a parcel, and a int, which is the bmp_id an string which is the key for a parcel, and a int, which is the bmp_id
 * It returns a tuple, the group_id and the position it is.
 *
 * @param[in] filename The name of the file to load
 *
 * @return a vector of land bmp solutions .
 *
 * @note This function does not perform any error checking or validation.
 */

void LandScenario::load_land_scenario(const std::string& filename)
{
    csv::CSVReader reader(filename);
    for(auto& row: reader) {
        try {
            LandSol new_entry;

            //  0             1            2                  3       4         5        6          7        8     9       10          11
            //BmpSubmittedId,AgencyId,StateUniqueIdentifier,StateId,BmpId,GeographyId,LoadSourceId,UnitId,Amount,isValid,ErrorMessage,RowIndex,
            // 12    13        14                 15          16        17                    18                19
            //Cost,LrsegId,LoadSourceIdOriginal,TotalAmount,Capital,NitrogenReduction,PhosphorusReduction,SedimentsReduction

            new_entry.agency = row["AgencyId"].get<int>();
            new_entry.bmp = row["BmpId"].get<int>();
            new_entry.lrseg = row["LrsegId"].get<int>();
            new_entry.load_src = row["LoadSourceIdOriginal"].get<int>();
            new_entry.amount = row["Amount"].get<double>();

            land_scenario.push_back(new_entry);

        } catch (std::exception& e) {
            // Catch any exceptions that derive from std::exception
            //std::cerr << "Caught exception: " << e.what() << std::endl;
        } catch (...) {
            // Catch any other exceptions
            std::cerr << "Caught unknown exception." << std::endl;
        }
    }
}

double LandScenario::compute_cost(int cost_profile_id) {

    double total_sum = 0.0;
    for (const auto& row : land_scenario) {
        std::string key = fmt::format("{}_{}", cost_profile_id, row.bmp);
        auto cost_per_unit = data_.get_bmp_cost(cost_profile_id, row.bmp);
        auto cost_per_unit2 = data_.get_bmp_cost(key);
        if(cost_per_unit != cost_per_unit2) {
            fmt::print("{} != {}", cost_per_unit, cost_per_unit2);
        }
        if ( cost_per_unit != -1) {
            double cost = cost_per_unit * row.amount;
            total_sum += cost;
            fmt::print("BMP: {}, Acres: {}, Cost per unit: {}, Subtotal {}, Accum. Cost: {}\n", row.bmp, row.amount, cost_per_unit, cost, total_sum);
        }
        else {
            fmt::print("Error en information presentation\n");
        }

    }
    return total_sum;
}


double LandScenario::compute_cost(const std::string& loads_filename, int cost_profile_id) {
    std::shared_ptr<arrow::io::ReadableFile> infile;
    PARQUET_ASSIGN_OR_THROW(infile, arrow::io::ReadableFile::Open(loads_filename, arrow::default_memory_pool()));
    std::unique_ptr<parquet::arrow::FileReader> reader;
    PARQUET_THROW_NOT_OK(parquet::arrow::OpenFile(infile, arrow::default_memory_pool(), &reader));

    std::shared_ptr<arrow::ChunkedArray> amountArray;
    std::shared_ptr<arrow::ChunkedArray> bmpIdArray;

    // Get the schema from the Parquet file
    std::shared_ptr<arrow::Schema> schema;
    PARQUET_THROW_NOT_OK(reader->GetSchema(&schema));
    
    // Find the index of the "Amount" column
    int amount_index = schema->GetFieldIndex("Amount");
    if (amount_index == -1) {
        throw std::runtime_error("Amount column not found in Parquet file.");
    }
    
    // Find the index of the "BmpId" column
    int bmpId_index = schema->GetFieldIndex("BmpId");
    if (bmpId_index == -1) {
        throw std::runtime_error("BmpId column not found in Parquet file.");
    }
    
    // Read the "Amount" column by index
    PARQUET_THROW_NOT_OK(reader->ReadColumn(amount_index, &amountArray));
    
    // Read the "BmpId" column by index
    PARQUET_THROW_NOT_OK(reader->ReadColumn(bmpId_index, &bmpIdArray));
    //PARQUET_THROW_NOT_OK(reader->GetColumnByName("Amount", &amountArray));
    //PARQUET_THROW_NOT_OK(reader->GetColumnByName("BmpId", &bmpIdArray));

    if (amountArray->length() != bmpIdArray->length()) {
        throw std::runtime_error("Length mismatch between Amount and BmpId columns.");
    }

    double total_cost = 0.0;

    // Assuming the columns are split into chunks of equal size
    for (int i = 0; i < amountArray->num_chunks(); i++) {
        auto amountValues = std::static_pointer_cast<arrow::DoubleArray>(amountArray->chunk(i));
        auto bmpIdValues = std::static_pointer_cast<arrow::Int32Array>(bmpIdArray->chunk(i));

        for (int64_t j = 0; j < amountValues->length(); j++) {
            if (!amountValues->IsNull(j) && !bmpIdValues->IsNull(j)) {
                int bmpId = bmpIdValues->Value(j);
                double amount = amountValues->Value(j);

                auto cost_per_unit = data_.get_bmp_cost(cost_profile_id, bmpId);
                total_cost += cost_per_unit * amount;
            }
        }
    }

    return total_cost;
}

/**
 * @brief Loads a land scenario
 *
 * This function takes an string which is the key for a parcel, and a int, which is the bmp_id an string which is the key for a parcel, and a int, which is the bmp_id
 * It returns a tuple, the group_id and the position it is.
 *
 * @param[in] filename The name of the file to load
 *
 * @return a vector of land bmp solutions .
 *
 * @note This function does not perform any error checking or validation.
 */

LandSol LandScenario::prepend(std::string key, int bmp, double amount) {
    LandSol new_row;
    new_row.lrseg = 0;
    new_row.agency = 0;
    new_row.load_src = 0;
    new_row.amount = 0;
    new_row.bmp = 0;
    return new_row;
}

std::vector<LandSol> LandScenario::get_land_scenario() {
    return land_scenario;
}
