
//
// Created by Gregorio Toscano on 3/27/23.
//
//True Forest!
// g++ land_scenario.cpp -std=c++20 -lfmt -lpthread -larrow -lparquet
#include "manure_scenario.h"
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

#include <arrow/compute/api.h>
#include <arrow/io/api.h>
#include <parquet/arrow/reader.h>
#include <unordered_map>
#include <vector>
#include <memory>

#include <fmt/core.h>

ManureScenario::ManureScenario(const DataReader& data_reader) : data_(data_reader) {
}



int ManureScenario::write( 
        const std::vector<ManureSol>& rows,
        const std::string& out_filename) {
    if (rows.size() == 0) {
        return 0;
    }


    parquet::schema::NodeVector fields;


    //BmpSubmittedId	BmpId	AgencyId	StateUniqueIdentifier	StateId	HasStateReference	CountyIdFrom	CountyIdTo	FipsFrom	FipsTo	AnimalGroupId	LoadSourceGroupId	UnitId	Amount	IsValid	ErrorMessage	RowIndex
    fields.push_back(parquet::schema::PrimitiveNode::Make(
            "BmpSubmittedId", parquet::Repetition::REQUIRED, parquet::Type::INT32, parquet::ConvertedType::INT_32
    ));
    fields.push_back(parquet::schema::PrimitiveNode::Make(
            "BmpId", parquet::Repetition::REQUIRED, parquet::Type::INT32, parquet::ConvertedType::INT_32
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

    ///////////////////////////////////////
    fields.push_back(parquet::schema::PrimitiveNode::Make(
            "HasStateReference", parquet::Repetition::REQUIRED, parquet::Type::BOOLEAN
    ));
    fields.push_back(parquet::schema::PrimitiveNode::Make(
            "CountyIdFrom", parquet::Repetition::REQUIRED, parquet::Type::INT32, parquet::ConvertedType::INT_32
    ));
    fields.push_back(parquet::schema::PrimitiveNode::Make(
            "CountyIdTo", parquet::Repetition::REQUIRED, parquet::Type::INT32, parquet::ConvertedType::INT_32
    ));
    fields.push_back(parquet::schema::PrimitiveNode::Make(
            "FipsFrom", parquet::Repetition::REQUIRED, parquet::Type::BYTE_ARRAY, parquet::ConvertedType::UTF8
    ));
    fields.push_back(parquet::schema::PrimitiveNode::Make(
            "FipsTo", parquet::Repetition::REQUIRED, parquet::Type::BYTE_ARRAY, parquet::ConvertedType::UTF8
    ));
    ///////////////////////////////////////

    fields.push_back(parquet::schema::PrimitiveNode::Make(
            "AnimalGroupId", parquet::Repetition::REQUIRED, parquet::Type::INT32, parquet::ConvertedType::INT_32
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
    std::shared_ptr<arrow::io::FileOutputStream> outfile;

    PARQUET_ASSIGN_OR_THROW(
            outfile,
            arrow::io::FileOutputStream::Open(out_filename));

    std::shared_ptr<parquet::schema::GroupNode> my_schema = std::static_pointer_cast<parquet::schema::GroupNode>(
            parquet::schema::GroupNode::Make("schema", parquet::Repetition::REQUIRED, fields));

    parquet::WriterProperties::Builder builder;
    //builder.compression(parquet::Compression::ZSTD);
    //builder.compression(parquet::Compression::SNAPPY);
    builder.version(parquet::ParquetVersion::PARQUET_1_0);
    parquet::StreamWriter os{
            parquet::ParquetFileWriter::Open(outfile, my_schema, builder.build())};


    int idx = 0;
    int counter = 0;
    double total_cost = 0.0;

    for (auto const&row: rows) {
        os<<counter+1<<row.bmp<<row.agency<<fmt::format("SU{}",counter)<<row.state<<true<<row.county_from<<row.county_to<<row.fips_from<<row.fips_to<<row.animal_grp<<row.load_src_grp<< row.unit<<row.amount<<true<<""<<counter+1<<parquet::EndRow;
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

void ManureScenario::load_scenario(const std::string& filename)
{
    csv::CSVReader reader(filename);
    for(auto& row: reader) {
        try {
            ManureSol new_entry;
            // BmpSubmittedId,BmpId,AgencyId,StateUniqueIdentifier,StateId,
            // HasStateReference,CountyIdFrom,CountyIdTo,FipsFrom,FipsTo,
            // AnimalGroupId,LoadSourceGroupId,UnitId,Amount,IsValid,
            // ErrorMessage,RowIndex
            new_entry.bmp = row["BmpId"].get<int>();
            new_entry.agency = row["AgencyId"].get<int>();
            new_entry.state= row["StateId"].get<int>();
            new_entry.county_from= row["CountyIdFrom"].get<int>();
            new_entry.county_to= row["CountyIdTo"].get<int>();
            new_entry.fips_from= row["FipsFrom"].get<std::string>();
            new_entry.fips_to= row["FipsTo"].get<std::string>();
            new_entry.animal_grp= row["AnimalGroupId"].get<int>();
            new_entry.load_src_grp = row["LoadSourceGroupId"].get<int>();
            new_entry.unit = row["UnitId"].get<int>();
            new_entry.amount = row["Amount"].get<double>();
            scenario_.push_back(new_entry);

        } catch (std::exception& e) {
            // Catch any exceptions that derive from std::exception
            //std::cerr << "Caught exception: " << e.what() << std::endl;
        } catch (...) {
            // Catch any other exceptions
            std::cerr << "Caught unknown exception." << std::endl;
        }
    }
}





double ManureScenario::compute_cost(int cost_profile_id) {

    double total_sum = 0.0;
    for (const auto& row : scenario_) {
        auto cost_per_unit = data_.get_bmp_cost(cost_profile_id, row.bmp);
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

double ManureScenario::compute_cost(const std::string& loads_filename, int cost_profile_id) {
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

std::vector<ManureSol> ManureScenario::get_scenario() {
    return scenario_;
}
