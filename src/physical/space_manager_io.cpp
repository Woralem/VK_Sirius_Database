#include "physical/space_manager_io.h"
#include <fstream>
#include <stdexcept>
#include <format>
#include <algorithm>
#include <vector>

// Loads the freelist from a .sp file into the provided vector cache.
void SpaceManager::load(const std::filesystem::path& sp_path, std::vector<FreeSpaceRecord>& inventory_cache) {
    std::ifstream file(sp_path, std::ios::binary);
    inventory_cache.clear();

    if (!file.is_open()) {
        // It's not an error if the file doesn't exist; it just means there's no free space recorded yet.
        return;
    }

    // Read each 10-byte record from the file.
    char buffer[RECORD_SIZE];
    while (file.read(buffer, RECORD_SIZE)) {
        FreeSpaceRecord record;
        memcpy(&record.offset, buffer, sizeof(uint64_t));
        memcpy(&record.length, buffer + sizeof(uint64_t), sizeof(uint16_t));
        inventory_cache.push_back(record);
    }
    
    // Sort the cache by length (ascending) to enable efficient "best-fit" searching.
    std::sort(inventory_cache.begin(), inventory_cache.end(), FreeSpaceRecord::Less{});
}

// Persists the in-memory cache of free space records to disk, overwriting the .sp file.
void SpaceManager::persist(const std::filesystem::path& sp_path, const std::vector<FreeSpaceRecord>& inventory_cache) {
    // Open the file in truncate mode to ensure we start with a clean slate.
    std::ofstream file(sp_path, std::ios::binary | std::ios::trunc);
    if (!file.is_open()) {
        throw std::runtime_error("Failed to open space file for writing: " + sp_path.string());
    }

    // Write each record from the cache into the file.
    for (const auto& record : inventory_cache) {
        char buffer[RECORD_SIZE];
        // The on-disk format is 8 bytes for offset followed by 2 bytes for length.
        memcpy(buffer, &record.offset, sizeof(uint64_t));
        memcpy(buffer + sizeof(uint64_t), &record.length, sizeof(uint16_t));
        
        file.write(buffer, RECORD_SIZE);
    }
}

// Adds a new record for a freed chunk of space into the cache, maintaining sort order.
void SpaceManager::add(std::vector<FreeSpaceRecord>& inventory_cache, uint64_t offset, uint16_t length) {
    if (length == 0) return;
    
    FreeSpaceRecord new_record = {offset, length};
    
    // Use lower_bound to find the correct insertion point to keep the vector sorted by length.
    // This makes adding an O(N) operation, but claiming space remains efficient.
    auto it = std::lower_bound(inventory_cache.begin(), inventory_cache.end(), new_record, FreeSpaceRecord::Less{});
    inventory_cache.insert(it, new_record);
}

// Finds the smallest available chunk that is large enough for the request, removes it, and returns it.
std::optional<FreeSpaceRecord> SpaceManager::claim(std::vector<FreeSpaceRecord>& inventory_cache, uint16_t required_length) {
    if (required_length == 0) return std::nullopt;

    // Use lower_bound with the transparent comparator to search for the first chunk
    // whose length is >= required_length. Because the cache is sorted, this is
    // the "best-fit" chunk.
    auto it = std::lower_bound(inventory_cache.begin(), inventory_cache.end(), required_length, FreeSpaceRecord::Less{});

    // If no suitable chunk is found, the iterator will be at the end.
    if (it == inventory_cache.end()) {
        return std::nullopt;
    }

    // A suitable chunk was found. Copy it, remove it from the cache, and return the copy.
    FreeSpaceRecord claimed_chunk = *it;
    inventory_cache.erase(it);

    return claimed_chunk;
}