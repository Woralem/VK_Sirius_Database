#include "storage/optimized_in_memory_storage.h"
#include "utils.h"
#include <format>
#include <cstdint>
#include <limits>

bool OptimizedInMemoryStorage::validateColumnName(std::string_view name, const TableOptions& options) const {
    if (name.empty() || name.length() > options.maxColumnNameLength) {
        return false;
    }
    for (char c : name) {
        if (!std::isalnum(c) && c != '_' && !options.additionalNameChars.contains(c)) {
            return false;
        }
    }
    return true;
}

const ColumnDef* OptimizedInMemoryStorage::getColumnDef(const Table& table, std::string_view colName) const {
    auto it = std::ranges::find_if(table.schema, [colName](const ColumnDef& def) {
        return def.name == colName;
    });
    return (it != table.schema.end()) ? &(*it) : nullptr;
}

bool OptimizedInMemoryStorage::validateValueForColumn(const Value& value, const ColumnDef& colDef) const {
    if (colDef.notNull && std::holds_alternative<std::monostate>(value)) {
        std::cout << "\033[91m[VALIDATION ERROR]\033[0m Column '" << colDef.name << "' cannot be null.\n";
        return false;
    }

    if (std::holds_alternative<std::monostate>(value)) return true;

    bool type_ok = false;
    switch (colDef.parsedType) {
        case DataType::INT:
            type_ok = std::holds_alternative<int64_t>(value);
            if (!type_ok) {
                std::cout << "\033[91m[VALIDATION ERROR]\033[0m Column '" << colDef.name
                         << "' expects INT but got different type.\n";
            }
            break;
        case DataType::DOUBLE:
            type_ok = std::holds_alternative<double>(value) || std::holds_alternative<int64_t>(value);
            if (!type_ok) {
                std::cout << "\033[91m[VALIDATION ERROR]\033[0m Column '" << colDef.name
                         << "' expects DOUBLE but got different type.\n";
            }
            break;
        case DataType::VARCHAR:
            type_ok = std::holds_alternative<std::string>(value);
            if (!type_ok) {
                std::cout << "\033[91m[VALIDATION ERROR]\033[0m Column '" << colDef.name
                         << "' expects VARCHAR but got different type.\n";
            }
            break;
        case DataType::BOOLEAN:
            type_ok = std::holds_alternative<bool>(value);
            if (!type_ok) {
                std::cout << "\033[91m[VALIDATION ERROR]\033[0m Column '" << colDef.name
                         << "' expects BOOLEAN but got different type.\n";
            }
            break;
        default:
            type_ok = true;
            break;
    }

    return type_ok;
}

// Остальной код остается без изменений...
bool OptimizedInMemoryStorage::isValidInteger(std::string_view str) const {
    if (str.empty()) return false;
    static const std::regex intRegex(R"(^-?\d+$)");
    return std::regex_match(str.data(), str.data() + str.size(), intRegex);
}

bool OptimizedInMemoryStorage::isValidDouble(std::string_view str) const {
    if (str.empty()) return false;
    static const std::regex doubleRegex(R"(^-?\d*(\.\d+)?([eE][-+]?\d+)?$)");
    return std::regex_match(str.data(), str.data() + str.size(), doubleRegex);
}

bool OptimizedInMemoryStorage::isValidBoolean(std::string_view str) const {
    std::string lower_str(str);
    std::ranges::transform(lower_str, lower_str.begin(), ::tolower);
    return lower_str == "true" || lower_str == "false" || lower_str == "1" || lower_str == "0";
}

bool OptimizedInMemoryStorage::stringToBoolean(std::string_view str) const {
    std::string lower_str(str);
    std::ranges::transform(lower_str, lower_str.begin(), ::tolower);
    return lower_str == "true" || lower_str == "1";
}

std::string OptimizedInMemoryStorage::valueToIndexKey(const json& value) const {
    if (value.is_string()) {
        return value.get<std::string>();
    }
    return value.dump();
}

void OptimizedInMemoryStorage::setJsonValue(json& row, std::string_view key, const Value& value) {
    std::visit([&row, &key](auto&& arg) {
        using T = std::decay_t<decltype(arg)>;
        if constexpr (std::same_as<T, std::monostate>) {
            row[std::string(key)] = nullptr;
        } else if constexpr (std::same_as<T, int64_t>) {
            row[std::string(key)] = arg;
        } else if constexpr (std::same_as<T, double>) {
            row[std::string(key)] = arg;
        } else {
            row[std::string(key)] = arg;
        }
    }, value);
}

json OptimizedInMemoryStorage::convertValue(const json& value, DataType fromType, DataType toType) const {
    if (value.is_null()) {
        return nullptr;
    }

    try {
        switch (toType) {
            case DataType::INT: {
                switch (fromType) {
                    case DataType::INT:
                        return value;
                    case DataType::DOUBLE:
                        return static_cast<int64_t>(std::round(value.get<double>()));
                    case DataType::VARCHAR: {
                        std::string str_val = value.get<std::string>();
                        if (isValidInteger(str_val)) {
                            return static_cast<int64_t>(std::stoll(str_val));
                        } else if (isValidDouble(str_val)) {
                            return static_cast<int64_t>(std::round(std::stod(str_val)));
                        } else {
                            std::cout << std::format("\033[93m[WARNING]\033[0m Cannot convert '{}' to INT, setting to NULL\n", str_val);
                            return nullptr;
                        }
                    }
                    case DataType::BOOLEAN:
                        return value.get<bool>() ? int64_t(1) : int64_t(0);
                    default:
                        return nullptr;
                }
            }

            case DataType::DOUBLE: {
                switch (fromType) {
                    case DataType::INT:
                        if (value.is_number_integer()) {
                            return static_cast<double>(value.get<int64_t>());
                        } else {
                            return static_cast<double>(value.get<int>());
                        }
                    case DataType::DOUBLE:
                        return value;
                    case DataType::VARCHAR: {
                        std::string str_val = value.get<std::string>();
                        if (isValidDouble(str_val) || isValidInteger(str_val)) {
                            return std::stod(str_val);
                        } else {
                            std::cout << std::format("\033[93m[WARNING]\033[0m Cannot convert '{}' to DOUBLE, setting to NULL\n", str_val);
                            return nullptr;
                        }
                    }
                    case DataType::BOOLEAN:
                        return value.get<bool>() ? 1.0 : 0.0;
                    default:
                        return nullptr;
                }
            }

            case DataType::VARCHAR: {
                switch (fromType) {
                    case DataType::INT:
                        return std::format("{}", value.get<int>());
                    case DataType::DOUBLE:
                        return std::format("{}", value.get<double>());
                    case DataType::VARCHAR:
                        return value;
                    case DataType::BOOLEAN:
                        return value.get<bool>() ? "true" : "false";
                    default:
                        return value.dump();
                }
            }

            case DataType::BOOLEAN: {
                switch (fromType) {
                    case DataType::INT:
                        return value.get<int>() != 0;
                    case DataType::DOUBLE:
                        return value.get<double>() != 0.0;
                    case DataType::VARCHAR: {
                        std::string str_val = value.get<std::string>();
                        if (isValidBoolean(str_val)) {
                            return stringToBoolean(str_val);
                        } else {
                            std::cout << std::format("\033[93m[WARNING]\033[0m Cannot convert '{}' to BOOLEAN, setting to NULL\n", str_val);
                            return nullptr;
                        }
                    }
                    case DataType::BOOLEAN:
                        return value;
                    default:
                        return nullptr;
                }
            }

            case DataType::DATE:
            case DataType::TIMESTAMP: {
                if (fromType == DataType::VARCHAR) {
                    return value;
                } else {
                    return convertValue(value, fromType, DataType::VARCHAR);
                }
            }

            default:
                std::cout << std::format("\033[91m[ERROR]\033[0m Unsupported target type: {}\n", dataTypeToString(toType));
                return nullptr;
        }
    } catch (const std::exception& e) {
        std::cout << std::format("\033[91m[ERROR]\033[0m Error converting value: {}, setting to NULL\n", e.what());
        return nullptr;
    }
}

DataType OptimizedInMemoryStorage::jsonTypeToDataType(const json& value) const {
    if (value.is_null()) return DataType::UNKNOWN_TYPE;
    if (value.is_number_integer()) return DataType::INT;
    if (value.is_number_float()) return DataType::DOUBLE;
    if (value.is_string()) return DataType::VARCHAR;
    if (value.is_boolean()) return DataType::BOOLEAN;
    return DataType::UNKNOWN_TYPE;
}

bool OptimizedInMemoryStorage::createTable(const std::string& tableName, const std::vector<ColumnDef*>& columns, const TableOptions& options) {
    if (tables.contains(tableName)) return false;

    auto& table = tables[tableName];
    table.options = options;
    table.lastGC = std::chrono::system_clock::now();

    for (auto* col : columns) {
        if (col->parsedType == DataType::UNKNOWN_TYPE ||
            !validateColumnName(col->name, options) ||
            (!options.allowedTypes.empty() && !options.allowedTypes.contains(col->parsedType))) {
            tables.erase(tableName);
            return false;
        }
        table.schema.push_back(*col);
        if(col->primaryKey) table.indexes[col->name];
    }
    return true;
}

bool OptimizedInMemoryStorage::insertRow(const std::string& tableName, const std::vector<std::string>& columns, const std::vector<Value>& values) {
    auto table_it = tables.find(tableName);
    if (table_it == tables.end()) return false;
    auto& table = table_it->second;

    std::unordered_map<std::string, Value> fullValueMap;

    for (const auto& colDef : table.schema) {
        fullValueMap[colDef.name] = std::monostate{};
    }

    if (columns.empty()) {
        if (values.size() != table.schema.size()) return false;
        for (size_t i = 0; i < values.size(); ++i) {
            fullValueMap[table.schema[i].name] = values[i];
        }
    } else {
        if (columns.size() != values.size()) return false;
        for (size_t i = 0; i < columns.size(); ++i) {
            fullValueMap[columns[i]] = values[i];
        }
    }

    for (const auto& colDef : table.schema) {
        const auto& value = fullValueMap[colDef.name];

        if (colDef.notNull && std::holds_alternative<std::monostate>(value)) {
            std::cout << "\033[91m[VALIDATION ERROR]\033[0m Column '" << colDef.name << "' cannot be null.\n";
            return false;
        }

        if (!std::holds_alternative<std::monostate>(value) && !validateValueForColumn(value, colDef)) {
            return false;
        }

        if (std::holds_alternative<std::string>(value) &&
            std::get<std::string>(value).length() > table.options.maxStringLength) {
            return false;
        }

        if (colDef.primaryKey && !std::holds_alternative<std::monostate>(value)) {
            json tempJson;
            setJsonValue(tempJson, colDef.name, value);
            auto keyValue = valueToIndexKey(tempJson[colDef.name]);

            if (table.indexes.contains(colDef.name) &&
                table.indexes.at(colDef.name).contains(keyValue) &&
                !table.indexes.at(colDef.name).at(keyValue).empty()) {
                std::cout << std::format("\033[91m[VALIDATION ERROR]\033[0m PRIMARY KEY constraint violated for column '{}' with value '{}'\n",
                                        colDef.name, keyValue);
                return false;
            }
        }
    }

    json row;
    for (const auto& [colName, value] : fullValueMap) {
        setJsonValue(row, colName, value);
    }

    size_t rowIndex = table.data.size();
    for (auto& [colName, index] : table.indexes) {
        if (row.contains(colName) && !row[colName].is_null()) {
            index[valueToIndexKey(row[colName])].push_back(rowIndex);
        }
    }
    table.data.push_back(std::move(row));
    return true;
}

int OptimizedInMemoryStorage::updateRows(const std::string& tableName,
                                         const std::vector<std::pair<std::string, Value>>& assignments,
                                         std::function<bool(const json&)> predicate) {
    auto it = tables.find(tableName);
    if (it == tables.end()) return 0;
    auto& table = it->second;

    int updatedCount = 0;
    std::vector<size_t> rowsToUpdate;

    for (size_t i = 0; i < table.data.size(); ++i) {
        if (!table.data[i].is_null() && predicate(table.data[i])) {
            rowsToUpdate.push_back(i);
        }
    }

    for (size_t i : rowsToUpdate) {
        bool valid = true;
        for (const auto& [colName, value] : assignments) {
            const ColumnDef* colDef = getColumnDef(table, colName);
            if (!colDef || !validateValueForColumn(value, *colDef)) {
                valid = false;
                break;
            }
            if (std::holds_alternative<std::string>(value) &&
                std::get<std::string>(value).length() > table.options.maxStringLength) {
                valid = false;
                break;
            }
            if (colDef->primaryKey) {
                json tempJson;
                setJsonValue(tempJson, colName, value);
                auto key_to_check = valueToIndexKey(tempJson[colName]);
                if(table.indexes.at(colName).contains(key_to_check) &&
                   !table.indexes.at(colName).at(key_to_check).empty() &&
                   table.indexes.at(colName).at(key_to_check)[0] != i) {
                    std::cout << std::format("\033[91m[VALIDATION ERROR]\033[0m UPDATE violates PRIMARY KEY constraint for key '{}'.\n", colName);
                    valid = false;
                    break;
                }
            }
        }
        if (!valid) continue;

        auto& row = table.data[i];
        for (const auto& [col, val] : assignments) {
            if (table.indexes.contains(col) && row.contains(col)) {
                auto& oldIndex = table.indexes[col][valueToIndexKey(row[col])];
                std::erase(oldIndex, i);
            }
        }

        for (const auto& [col, val] : assignments) {
            setJsonValue(row, col, val);
            if (table.indexes.contains(col)) {
                table.indexes[col][valueToIndexKey(row[col])].push_back(i);
            }
        }
        updatedCount++;
    }
    return updatedCount;
}

int OptimizedInMemoryStorage::deleteRows(const std::string& tableName, std::function<bool(const json&)> predicate) {
    auto it = tables.find(tableName);
    if (it == tables.end()) return 0;
    auto& table = it->second;

    std::vector<size_t> toDeleteIndices;
    for (size_t i = 0; i < table.data.size(); ++i) {
        if (!table.data[i].is_null() && predicate(table.data[i])) {
            toDeleteIndices.push_back(i);
        }
    }
    if (toDeleteIndices.empty()) return 0;

    int deletedCount = toDeleteIndices.size();

    // Создаем новый массив данных, исключая удаленные строки.
    // Это проще и безопаснее, чем удаление на месте со сдвигом.
    json newData = json::array();
    newData.get_ref<json::array_t&>().reserve(table.data.size() - deletedCount);

    // Перестраиваем индексы
    for(auto& pair : table.indexes) {
        pair.second.clear();
    }

    size_t newIndex = 0;
    for(size_t i = 0; i < table.data.size(); ++i) {
        // Проверяем, нужно ли удалять этот индекс
        if(std::ranges::find(toDeleteIndices, i) == toDeleteIndices.end()) {
            // Если не удаляем, копируем строку и обновляем индексы
            auto& row = table.data[i];
            newData.push_back(row);
            for(auto& [colName, index] : table.indexes) {
                if(row.contains(colName)) {
                    index[valueToIndexKey(row[colName])].push_back(newIndex);
                }
            }
            newIndex++;
        }
    }

    table.data = std::move(newData);

    return deletedCount;
}

json OptimizedInMemoryStorage::selectRows(const std::string& tableName,
                                          const std::vector<std::string>& columns,
                                          std::function<bool(const json&)> predicate) {
    json result;
    result["status"] = "success";
    result["table_name"] = tableName;

    auto it = tables.find(tableName);
    if (it == tables.end()) {
        result["status"] = "error";
        result["message"] = std::format("Table '{}' does not exist", tableName);
        return result;
    }

    const auto& table = it->second;
    const auto& tableData = table.data;

    std::vector<json> matchingRows;
    for (const auto& row : tableData) {
        if (!row.is_null() && predicate(row)) {
            matchingRows.push_back(row);
        }
    }

    if (matchingRows.empty()) {
        std::vector<std::string> headerNames;
        if (columns.empty() || (columns.size() == 1 && columns[0] == "*")) {
            for (const auto& col : table.schema) {
                headerNames.push_back(col.name);
            }
        } else {
            headerNames = columns;
        }

        json header = json::array();
        for (size_t i = 0; i < headerNames.size(); ++i) {
            json headerCell;
            headerCell["content"] = headerNames[i];
            headerCell["id"] = std::format("col_{}", i);

            const ColumnDef* colDef = getColumnDef(table, headerNames[i]);
            if (colDef) {
                headerCell["type"] = std::string(dataTypeToString(colDef->parsedType));
            } else {
                headerCell["type"] = "UNKNOWN";
            }

            header.push_back(headerCell);
        }
        result["header"] = header;
        result["cells"] = json::array();
        return result;
    }

   std::vector<std::string> headerNames;
    if (columns.empty() || (columns.size() == 1 && columns[0] == "*")) {
        std::set<std::string> availableColumns;
        for (auto& [key, value] : matchingRows[0].items()) {
            availableColumns.insert(key);
        }

        for (const auto& col : table.schema) {
            if (availableColumns.count(col.name)) {
                headerNames.push_back(col.name);
            }
        }

        for (const auto& colName : availableColumns) {
            if (std::find(headerNames.begin(), headerNames.end(), colName) == headerNames.end()) {
                headerNames.push_back(colName);
            }
        }
    } else {
        headerNames = columns;
    }

    json header = json::array();
    for (size_t i = 0; i < headerNames.size(); ++i) {
        json headerCell;
        headerCell["content"] = headerNames[i];
        headerCell["id"] = std::format("col_{}", i);

        const ColumnDef* colDef = getColumnDef(table, headerNames[i]);
        if (colDef) {
            headerCell["type"] = std::string(dataTypeToString(colDef->parsedType));
        } else {
            headerCell["type"] = "UNKNOWN";
        }

        header.push_back(headerCell);
    }
    result["header"] = header;

    json cells = json::array();
    for (size_t rowIndex = 0; rowIndex < matchingRows.size(); ++rowIndex) {
        const auto& row = matchingRows[rowIndex];
        json cellRow = json::array();

        for (size_t colIndex = 0; colIndex < headerNames.size(); ++colIndex) {
            const std::string& columnName = headerNames[colIndex];
            json cell;

            if (row.contains(columnName)) {
                cell["content"] = row[columnName];
            } else {
                cell["content"] = nullptr;
            }

            cell["id"] = std::format("cell_{}_{}", rowIndex, colIndex);
            cellRow.push_back(cell);
        }

        cells.push_back(cellRow);
    }
    result["cells"] = cells;

    return result;
}

bool OptimizedInMemoryStorage::renameTable(const std::string& oldName, const std::string& newName) {
    auto it = tables.find(oldName);
    if (it == tables.end()) return false;
    if (tables.contains(newName)) return false;

    auto node = tables.extract(it);
    node.key() = newName;
    tables.insert(std::move(node));
    return true;
}

bool OptimizedInMemoryStorage::renameColumn(const std::string& tableName,
                                            const std::string& oldColumnName,
                                            const std::string& newColumnName) {
    auto it = tables.find(tableName);
    if (it == tables.end()) return false;

    auto& table = it->second;

    bool columnExists = std::ranges::any_of(table.schema, [&oldColumnName](const auto& col) {
        return col.name == oldColumnName;
    });

    if (!columnExists) return false;

    if (std::ranges::any_of(table.schema, [&newColumnName](const auto& col) {
        return col.name == newColumnName;
    })) {
        return false;
    }

    if (!validateColumnName(newColumnName, table.options)) return false;

    // Используем ranges для поиска и обновления
    auto col_it = std::ranges::find_if(table.schema, [&oldColumnName](auto& col) {
        return col.name == oldColumnName;
    });

    if (col_it != table.schema.end()) {
        col_it->name = newColumnName;
    }

    // Обновляем данные
    for (auto& row : table.data) {
        if (!row.is_null() && row.contains(oldColumnName)) {
            row[newColumnName] = row[oldColumnName];
            row.erase(oldColumnName);
        }
    }

    // Обновляем индексы
    if (table.indexes.contains(oldColumnName)) {
        auto node = table.indexes.extract(oldColumnName);
        node.key() = newColumnName;
        table.indexes.insert(std::move(node));
    }

    return true;
}

bool OptimizedInMemoryStorage::addColumn(const std::string& tableName, const ColumnDef* columnDef) {
    auto it = tables.find(tableName);
    if (it == tables.end()) {
        std::cout << std::format("\033[91m[ERROR]\033[0m Table '{}' does not exist.\n", tableName);
        return false;
    }

    auto& table = it->second;

    bool columnExists = std::ranges::any_of(table.schema, [&columnDef](const auto& col) {
        return col.name == columnDef->name;
    });

    if (columnExists) {
        std::cout << std::format("\033[91m[ERROR]\033[0m Column '{}' already exists.\n", columnDef->name);
        return false;
    }

    if (!validateColumnName(columnDef->name, table.options)) {
        std::cout << std::format("\033[91m[ERROR]\033[0m Invalid column name '{}'.\n", columnDef->name);
        return false;
    }

    if (!table.options.allowedTypes.empty() && !table.options.allowedTypes.contains(columnDef->parsedType)) {
        std::cout << std::format("\033[91m[ERROR]\033[0m Type '{}' is not allowed for this table.\n",
                                dataTypeToString(columnDef->parsedType));
        return false;
    }

    if (columnDef->primaryKey && table.data.size() > 0) {
        std::cout << std::format("\033[91m[ERROR]\033[0m Cannot add PRIMARY KEY column '{}' to table with existing data.\n",
                                columnDef->name);
        return false;
    }

    std::cout << std::format("\033[96m[INFO]\033[0m Adding column '{}' of type {} to table '{}'...\n",
                            columnDef->name, dataTypeToString(columnDef->parsedType), tableName);

    table.schema.push_back(*columnDef);

    if (columnDef->primaryKey) {
        table.indexes[columnDef->name];
    }

    for (size_t i = 0; i < table.data.size(); ++i) {
        auto& row = table.data[i];
        if (!row.is_null()) {
            if (columnDef->notNull) {
                switch (columnDef->parsedType) {
                    case DataType::INT:
                        row[columnDef->name] = int64_t(0);
                        break;
                    case DataType::DOUBLE:
                        row[columnDef->name] = 0.0;
                        break;
                    case DataType::VARCHAR:
                        row[columnDef->name] = "";
                        break;
                    case DataType::BOOLEAN:
                        row[columnDef->name] = false;
                        break;
                    default:
                        row[columnDef->name] = nullptr;
                        break;
                }
            } else {
                row[columnDef->name] = nullptr;
            }
            if (columnDef->primaryKey && !row[columnDef->name].is_null()) {
                table.indexes[columnDef->name][valueToIndexKey(row[columnDef->name])].push_back(i);
            }
        }
    }

    std::cout << std::format("\033[92m[SUCCESS]\033[0m Column '{}' added successfully to {} existing rows!\n",
                            columnDef->name, table.data.size());
    return true;
}


bool OptimizedInMemoryStorage::alterColumnType(const std::string& tableName,
                                               const std::string& columnName,
                                               DataType newType) {
    auto it = tables.find(tableName);
    if (it == tables.end()) {
        std::cout << std::format("\033[91m[ERROR]\033[0m Table '{}' does not exist.\n", tableName);
        return false;
    }

    auto& table = it->second;

    ColumnDef* colDef = nullptr;
    auto col_it = std::ranges::find_if(table.schema, [&columnName](const auto& col) {
        return col.name == columnName;
    });

    if (col_it == table.schema.end()) {
        std::cout << std::format("\033[91m[ERROR]\033[0m Column '{}' does not exist.\n", columnName);
        return false;
    }

    colDef = &(*col_it);

    if (!table.options.allowedTypes.empty() && !table.options.allowedTypes.contains(newType)) {
        std::cout << std::format("\033[91m[ERROR]\033[0m Type '{}' is not allowed for this table.\n",
                                dataTypeToString(newType));
        return false;
    }

    DataType oldType = colDef->parsedType;

    std::cout << std::format("\033[96m[INFO]\033[0m Converting column '{}' from {} to {}...\n",
                            columnName, dataTypeToString(oldType), dataTypeToString(newType));

    int convertedCount = 0;
    int nullCount = 0;
    int totalCount = 0;

    for (auto& row : table.data) {
        if (!row.is_null() && row.contains(columnName)) {
            totalCount++;
            json oldValue = row[columnName];

            if (oldValue.is_null()) {
                continue;
            }

            DataType actualOldType = jsonTypeToDataType(oldValue);
            json newValue = convertValue(oldValue, actualOldType, newType);

            if (newValue.is_null() && !oldValue.is_null()) {
                nullCount++;
            } else {
                convertedCount++;
            }

            row[columnName] = newValue;
        }
    }

    colDef->parsedType = newType;
    colDef->dataType = std::string(dataTypeToString(newType));

    std::cout << "\033[92m[SUCCESS]\033[0m Column type changed successfully!\n";
    std::cout << std::format("\033[96m[STATS]\033[0m Total rows: {}, Converted: {}, Set to NULL: {}\n",
                            totalCount, convertedCount, nullCount);

    return true;
}

bool OptimizedInMemoryStorage::dropColumn(const std::string& tableName, const std::string& columnName) {
    auto it = tables.find(tableName);
    if (it == tables.end()) {
        std::cout << std::format("\033[91m[ERROR]\033[0m Table '{}' does not exist.\n", tableName);
        return false;
    }

    auto& table = it->second;

    bool columnExists = std::ranges::any_of(table.schema, [&columnName](const auto& col) {
        return col.name == columnName;
    });

    if (!columnExists) {
        std::cout << std::format("\033[91m[ERROR]\033[0m Column '{}' does not exist.\n", columnName);
        return false;
    }

    if (table.schema.size() <= 1) {
        std::cout << "\033[91m[ERROR]\033[0m Cannot drop the last column from table.\n";
        return false;
    }

    std::cout << std::format("\033[96m[INFO]\033[0m Dropping column '{}' from table '{}'...\n",
                            columnName, tableName);

    std::erase_if(table.schema, [&columnName](const ColumnDef& col) {
        return col.name == columnName;
    });

    for (auto& row : table.data) {
        if (!row.is_null() && row.contains(columnName)) {
            row.erase(columnName);
        }
    }

    if (table.indexes.contains(columnName)) {
        table.indexes.erase(columnName);
    }

    std::cout << std::format("\033[92m[SUCCESS]\033[0m Column '{}' dropped successfully!\n", columnName);
    return true;
}

bool OptimizedInMemoryStorage::dropTable(const std::string& tableName) {
    auto it = tables.find(tableName);
    if (it == tables.end()) {
        return false;
    }

    tables.erase(it);
    std::cout << std::format("\033[92m[SUCCESS]\033[0m Table '{}' dropped successfully.\n", tableName);
    return true;
}

void OptimizedInMemoryStorage::performGarbageCollection(Table& table) {
    for (auto& [colName, index] : table.indexes) {
        std::erase_if(index, [](const auto& pair) {
            return pair.second.empty();
        });
    }
}