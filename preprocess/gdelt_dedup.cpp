#include <algorithm>
#include <cctype>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <string>
#include <unordered_set>

#include <nlohmann/json.hpp>

namespace fs = std::filesystem;
using json = nlohmann::json;

struct Stats {
    std::size_t total{0};
    std::size_t parsed{0};
    std::size_t written{0};
    std::size_t duplicates{0};
    std::size_t parse_errors{0};
    std::size_t empty_lines{0};
};

std::string normalize_ascii_lower(const std::string& input) {
    std::string out;
    out.reserve(input.size());
    bool in_space = false;

    for (unsigned char ch : input) {
        if (std::isspace(ch) != 0) {
            if (!in_space) {
                out.push_back(' ');
                in_space = true;
            }
            continue;
        }

        in_space = false;
        out.push_back(static_cast<char>(std::tolower(ch)));
    }

    // Trim leading/trailing spaces introduced by normalization.
    while (!out.empty() && out.front() == ' ') {
        out.erase(out.begin());
    }
    while (!out.empty() && out.back() == ' ') {
        out.pop_back();
    }
    return out;
}

std::string normalize_urlish(std::string url) {
    // Lowercase ASCII characters and strip trailing slashes.
    std::transform(
        url.begin(),
        url.end(),
        url.begin(),
        [](unsigned char ch) { return static_cast<char>(std::tolower(ch)); }
    );
    while (!url.empty() && url.back() == '/') {
        url.pop_back();
    }
    return url;
}

std::string build_key(const json& record) {
    std::string text_norm;
    if (auto it = record.find("text"); it != record.end() && it->is_string()) {
        text_norm = normalize_ascii_lower(it->get<std::string>());
    }

    std::string title_norm;
    if (auto it = record.find("title"); it != record.end() && it->is_string()) {
        title_norm = normalize_ascii_lower(it->get<std::string>());
    }

    std::string url_norm;
    if (auto it = record.find("url"); it != record.end() && it->is_string()) {
        url_norm = normalize_urlish(it->get<std::string>());
    }

    if (!text_norm.empty()) {
        // For very short snippets, add URL to reduce collisions.
        if (text_norm.size() < 80 && !url_norm.empty()) {
            return text_norm + "|url|" + url_norm;
        }
        return text_norm;
    }

    if (!title_norm.empty()) {
        // Title-only keys need URL to avoid grouping unrelated posts.
        if (!url_norm.empty()) {
            return title_norm + "|url|" + url_norm;
        }
        return title_norm;
    }

    if (!url_norm.empty()) {
        return "url|" + url_norm;
    }

    if (auto it = record.find("id"); it != record.end() && it->is_string()) {
        return "id|" + it->get<std::string>();
    }

    return {};
}

int main(int argc, char* argv[]) {
    fs::path input_path = argc > 1 ? fs::path(argv[1]) : fs::path("data_crawl") / "gdelt.jsonl";
    fs::path output_path = argc > 2 ? fs::path(argv[2]) : fs::path("data_preprocessed") / "gdelt.jsonl";

    if (!fs::exists(input_path)) {
        std::cerr << "Input file not found: " << input_path << '\n';
        return 1;
    }

    if (output_path.has_parent_path()) {
        std::error_code ec;
        fs::create_directories(output_path.parent_path(), ec);
        if (ec) {
            std::cerr << "Failed to create output directory: " << ec.message() << '\n';
            return 1;
        }
    }

    std::ifstream in(input_path);
    if (!in.is_open()) {
        std::cerr << "Failed to open input file: " << input_path << '\n';
        return 1;
    }

    std::ofstream out(output_path);
    if (!out.is_open()) {
        std::cerr << "Failed to open output file: " << output_path << '\n';
        return 1;
    }

    Stats stats;
    std::unordered_set<std::string> seen_keys;
    std::string line;

    while (std::getline(in, line)) {
        ++stats.total;
        if (line.empty()) {
            ++stats.empty_lines;
            continue;
        }

        json record;
        try {
            record = json::parse(line);
        } catch (const std::exception& ex) {
            ++stats.parse_errors;
            std::cerr << "Skipping malformed JSON (line " << stats.total << "): " << ex.what() << '\n';
            continue;
        }

        ++stats.parsed;
        std::string key = build_key(record);
        if (key.empty()) {
            key = "line|" + std::to_string(stats.total);
        }

        auto [_, inserted] = seen_keys.emplace(std::move(key));
        if (!inserted) {
            ++stats.duplicates;
            continue;
        }

        out << line << '\n';
        ++stats.written;
    }

    std::cout << "Processed:       " << stats.total << '\n';
    std::cout << "Parsed:          " << stats.parsed << '\n';
    std::cout << "Written (unique):" << stats.written << '\n';
    std::cout << "Duplicates:      " << stats.duplicates << '\n';
    std::cout << "Parse errors:    " << stats.parse_errors << '\n';
    std::cout << "Empty lines:     " << stats.empty_lines << '\n';

    return 0;
}
