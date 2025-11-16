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

struct DedupResult {
    Stats stats;
    bool success{false};
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

DedupResult dedup_file(const fs::path& input_path, const fs::path& output_path) {
    Stats stats;

    if (!fs::exists(input_path)) {
        std::cerr << "Input file not found: " << input_path << '\n';
        return {stats, false};
    }

    if (output_path.has_parent_path()) {
        std::error_code ec;
        fs::create_directories(output_path.parent_path(), ec);
        if (ec) {
            std::cerr << "Failed to create output directory: " << ec.message() << '\n';
            return {stats, false};
        }
    }

    std::ifstream in(input_path);
    if (!in.is_open()) {
        std::cerr << "Failed to open input file: " << input_path << '\n';
        return {stats, false};
    }

    std::ofstream out(output_path);
    if (!out.is_open()) {
        std::cerr << "Failed to open output file: " << output_path << '\n';
        return {stats, false};
    }

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

    return {stats, true};
}

void print_stats(const Stats& stats, const fs::path& input_path) {
    std::cout << "[dedup] " << input_path << '\n';
    std::cout << "  processed:        " << stats.total << '\n';
    std::cout << "  parsed:           " << stats.parsed << '\n';
    std::cout << "  written (unique): " << stats.written << '\n';
    std::cout << "  duplicates:       " << stats.duplicates << '\n';
    std::cout << "  parse errors:     " << stats.parse_errors << '\n';
    std::cout << "  empty lines:      " << stats.empty_lines << '\n';
}

fs::path pick_default_input_dir() {
    const fs::path crawl = "data_crawl";
    const fs::path preprocessed = "data_preprocessed";
    std::error_code ec;
    if (fs::exists(crawl, ec) && fs::is_directory(crawl, ec)) {
        return crawl;
    }
    if (fs::exists(preprocessed, ec) && fs::is_directory(preprocessed, ec)) {
        return preprocessed;
    }
    return {};
}

fs::path default_output_dir_for(const fs::path& input_dir) {
    if (input_dir.filename() == "data_preprocessed") {
        return "data_preprocessed_dedup";
    }
    return "data_preprocessed";
}

bool paths_equivalent(const fs::path& a, const fs::path& b) {
    std::error_code ec;
    return fs::equivalent(a, b, ec);
}

int main(int argc, char* argv[]) {
    fs::path input_dir = pick_default_input_dir();
    if (input_dir.empty()) {
        std::cerr << "No default input directory found. Create data or pass paths explicitly.\n";
        return 1;
    }
    fs::path output_dir = default_output_dir_for(input_dir);

    bool run_all = (argc == 1);
    fs::path input_path;
    fs::path output_path;

    if (argc > 1 && std::string(argv[1]) == "--all") {
        run_all = true;
        if (argc > 2) {
            input_dir = argv[2];
        }
        if (argc > 3) {
            output_dir = argv[3];
        }
    } else if (!run_all) {
        if (argc > 1) {
            input_path = argv[1];
        }
        if (argc > 2) {
            output_path = argv[2];
        }
        if (input_path.empty()) {
            std::cerr << "Single-file mode requires an input path.\n";
            return 1;
        }
        if (output_path.empty()) {
            fs::path out_name = input_path.filename();
            if (out_name.empty()) {
                out_name = "deduped.jsonl";
            }
            output_path = output_dir / out_name;
        }
        if (paths_equivalent(input_path, output_path)) {
            fs::path new_name = input_path.stem();
            new_name += ".dedup";
            new_name += input_path.extension();
            output_path = input_path.parent_path() / new_name;
            std::cerr << "Output path matched input; redirecting to " << output_path << '\n';
        }
    }

    if (run_all) {
        if (!fs::exists(input_dir) || !fs::is_directory(input_dir)) {
            std::cerr << "Input directory not found: " << input_dir << '\n';
            return 1;
        }

        std::error_code ec;
        fs::create_directories(output_dir, ec);
        if (ec) {
            std::cerr << "Failed to create output directory: " << ec.message() << '\n';
            return 1;
        }

        Stats total;
        bool processed_any = false;

        for (const auto& entry : fs::directory_iterator(input_dir)) {
            if (!entry.is_regular_file()) {
                continue;
            }
            if (entry.path().extension() != ".jsonl") {
                continue;
            }
            processed_any = true;
            fs::path out_path = output_dir / entry.path().filename();
            DedupResult result = dedup_file(entry.path(), out_path);
            print_stats(result.stats, entry.path());

            if (!result.success) {
                continue;
            }

            total.total += result.stats.total;
            total.parsed += result.stats.parsed;
            total.written += result.stats.written;
            total.duplicates += result.stats.duplicates;
            total.parse_errors += result.stats.parse_errors;
            total.empty_lines += result.stats.empty_lines;
        }

        if (!processed_any) {
            std::cerr << "No .jsonl files found in: " << input_dir << '\n';
            return 1;
        }

        std::cout << "[summary]\n";
        std::cout << "  processed:        " << total.total << '\n';
        std::cout << "  parsed:           " << total.parsed << '\n';
        std::cout << "  written (unique): " << total.written << '\n';
        std::cout << "  duplicates:       " << total.duplicates << '\n';
        std::cout << "  parse errors:     " << total.parse_errors << '\n';
        std::cout << "  empty lines:      " << total.empty_lines << '\n';
        return 0;
    }

    DedupResult result = dedup_file(input_path, output_path);
    print_stats(result.stats, input_path);

    return result.success ? 0 : 1;
}
