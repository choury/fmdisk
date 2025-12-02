#include "common.h"
#include "fmdisk.h"
#include "mock_backend.h"
#include "../src/fuse.h"
#include "../src/file.h"
#include "utils.h"

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <vector>

#include <json-c/json.h>
#include <sqlite3.h>

const std::string secret = "integration-secret";
std::string cache_dir;
struct FuseMount {
    struct fuse_conn_info conn {};
    struct fuse_config cfg {};
    void* userdata = nullptr;
};

struct Command {
    std::string name;
    std::unordered_map<std::string, std::string> args;
    int line = 0;
};

enum class HandleKind {
    File,
    Dir
};

struct HandleState {
    fuse_file_info info {};
    HandleKind kind = HandleKind::File;
};

struct ExecutionContext {
    FuseMount mount;
    bool mounted = false;
    std::unordered_map<std::string, HandleState> handles;
    std::string script_name;
};

[[noreturn]] void fail(const ExecutionContext& ctx, const Command& cmd, const std::string& message) {
    std::ostringstream oss;
    oss << ctx.script_name << ":" << cmd.line << " [" << cmd.name << "] " << message;
    throw std::runtime_error(oss.str());
}

std::string require_arg(const Command& cmd, const std::string& key) {
    auto it = cmd.args.find(key);
    if(it == cmd.args.end()) {
        std::ostringstream oss;
        oss << "missing required argument '" << key << "'";
        throw std::runtime_error(oss.str());
    }
    return it->second;
}

std::optional<std::string> optional_arg(const Command& cmd, const std::string& key) {
    auto it = cmd.args.find(key);
    if(it == cmd.args.end()) {
        return std::nullopt;
    }
    return it->second;
}

bool parse_bool(const std::string& value) {
    if(value == "1" || value == "true" || value == "TRUE" || value == "yes" || value == "YES" || value == "on" || value == "ON") {
        return true;
    }
    if(value == "0" || value == "false" || value == "FALSE" || value == "no" || value == "NO" || value == "off" || value == "OFF") {
        return false;
    }
    throw std::runtime_error("invalid boolean '" + value + "'");
}

long parse_long(const std::string& value, int base = 0) {
    char* end = nullptr;
    errno = 0;
    long result = std::strtol(value.c_str(), &end, base);
    if(end == value.c_str() || *end != '\0' || errno != 0) {
        throw std::runtime_error("invalid integer '" + value + "'");
    }
    return result;
}

unsigned long parse_ulong(const std::string& value, int base = 0) {
    char* end = nullptr;
    errno = 0;
    unsigned long result = std::strtoul(value.c_str(), &end, base);
    if(end == value.c_str() || *end != '\0' || errno != 0) {
        throw std::runtime_error("invalid unsigned integer '" + value + "'");
    }
    return result;
}

std::string decode_escapes(const std::string& input) {
    std::string decoded;
    decoded.reserve(input.size());
    for(size_t i = 0; i < input.size(); ++i) {
        char c = input[i];
        if(c == '\\' && i + 1 < input.size()) {
            char next = input[++i];
            switch(next) {
                case 'n': decoded.push_back('\n'); break;
                case 'r': decoded.push_back('\r'); break;
                case 't': decoded.push_back('\t'); break;
                case '\\': decoded.push_back('\\'); break;
                case '"': decoded.push_back('"'); break;
                case '0': decoded.push_back('\0'); break;
                default: decoded.push_back(next); break;
            }
        } else {
            decoded.push_back(c);
        }
    }
    return decoded;
}

struct JsonPathComponent {
    bool is_index = false;
    std::string key;
    size_t index = 0;
};

std::vector<JsonPathComponent> parse_json_path(const std::string& path) {
    std::vector<JsonPathComponent> components;
    std::string token;
    for(size_t i = 0; i < path.size();) {
        char c = path[i];
        if(c == '.') {
            if(token.empty()) {
                throw std::runtime_error("empty segment in path '" + path + "'");
            }
            components.push_back(JsonPathComponent{false, token, 0});
            token.clear();
            ++i;
            continue;
        }
        if(c == '[') {
            if(!token.empty()) {
                components.push_back(JsonPathComponent{false, token, 0});
                token.clear();
            }
            ++i;
            if(i >= path.size()) {
                throw std::runtime_error("unterminated index in path '" + path + "'");
            }
            size_t start = i;
            bool has_digit = false;
            while(i < path.size() && path[i] != ']') {
                if(!std::isdigit(static_cast<unsigned char>(path[i]))) {
                    throw std::runtime_error("non-numeric array index in path '" + path + "'");
                }
                has_digit = true;
                ++i;
            }
            if(i >= path.size() || path[i] != ']') {
                throw std::runtime_error("unterminated index in path '" + path + "'");
            }
            if(!has_digit) {
                throw std::runtime_error("empty array index in path '" + path + "'");
            }
            std::string index_str = path.substr(start, i - start);
            size_t index = static_cast<size_t>(std::stoul(index_str));
            components.push_back(JsonPathComponent{true, {}, index});
            ++i; // consume ']'
            continue;
        }
        if(c == ']') {
            throw std::runtime_error("unexpected ']' in path '" + path + "'");
        }
        token.push_back(c);
        ++i;
    }
    if(!token.empty()) {
        components.push_back(JsonPathComponent{false, token, 0});
    }
    return components;
}

json_object* resolve_json_path(json_object* root, const std::string& path) {
    if(path.empty()) {
        return root;
    }
    json_object* current = root;
    auto components = parse_json_path(path);
    for(const auto& component : components) {
        if(component.is_index) {
            if(json_object_get_type(current) != json_type_array) {
                throw std::runtime_error("segment expects array but found non-array");
            }
            size_t length = json_object_array_length(current);
            if(component.index >= length) {
                std::ostringstream oss;
                oss << "array index " << component.index << " out of range (size " << length << ")";
                throw std::runtime_error(oss.str());
            }
            current = json_object_array_get_idx(current, component.index);
            continue;
        }
        if(json_object_get_type(current) != json_type_object) {
            throw std::runtime_error("segment expects object but found non-object");
        }
        json_object* next = nullptr;
        if(!json_object_object_get_ex(current, component.key.c_str(), &next)) {
            throw std::runtime_error("missing key '" + component.key + "'");
        }
        current = next;
    }
    return current;
}

const std::unordered_map<std::string, int> kErrnoMap = {
    {"EACCES", EACCES}, {"EAGAIN", EAGAIN}, {"EBADF", EBADF}, {"EBUSY", EBUSY}, {"EEXIST", EEXIST},
    {"EINTR", EINTR}, {"EINVAL", EINVAL}, {"EIO", EIO}, {"EISDIR", EISDIR}, {"EMFILE", EMFILE},
    {"ENAMETOOLONG", ENAMETOOLONG}, {"ENOENT", ENOENT}, {"ENOSYS", ENOSYS}, {"ENOTDIR", ENOTDIR},
    {"ENOTEMPTY", ENOTEMPTY}, {"EROFS", EROFS}, {"EXDEV", EXDEV}
};

std::optional<int> parse_expected_errno(const Command& cmd, const std::string& key) {
    auto value_opt = optional_arg(cmd, key);
    if(!value_opt.has_value()) {
        return std::nullopt;
    }
    const std::string& value = value_opt.value();
    if(!value.empty() && (std::isalpha(static_cast<unsigned char>(value[0])) || value[0] == '_')) {
        auto it = kErrnoMap.find(value);
        if(it == kErrnoMap.end()) {
            throw std::runtime_error("unknown errno '" + value + "'");
        }
        return it->second;
    }
    long numeric = parse_long(value, 0);
    if(numeric < 0) {
        numeric = -numeric;
    }
    return static_cast<int>(numeric);
}

HandleState& require_handle_state(ExecutionContext& ctx, const Command& cmd, const std::string& key = "handle") {
    std::string handle_name = require_arg(cmd, key);
    auto it = ctx.handles.find(handle_name);
    if(it == ctx.handles.end()) {
        std::ostringstream oss;
        oss << "unknown handle '" << handle_name << "'";
        throw std::runtime_error(oss.str());
    }
    return it->second;
}

HandleState& require_file_handle(ExecutionContext& ctx, const Command& cmd, const std::string& key = "handle") {
    HandleState& state = require_handle_state(ctx, cmd, key);
    if(state.kind != HandleKind::File) {
        std::ostringstream oss;
        oss << "handle '" << require_arg(cmd, key) << "' is not a file handle";
        throw std::runtime_error(oss.str());
    }
    return state;
}

HandleState& require_dir_handle(ExecutionContext& ctx, const Command& cmd, const std::string& key = "handle") {
    HandleState& state = require_handle_state(ctx, cmd, key);
    if(state.kind != HandleKind::Dir) {
        std::ostringstream oss;
        oss << "handle '" << require_arg(cmd, key) << "' is not a directory handle";
        throw std::runtime_error(oss.str());
    }
    return state;
}

void store_handle(ExecutionContext& ctx, const Command& cmd, const std::string& key, const std::string& path, HandleKind kind, const fuse_file_info& info) {
    std::string handle_name = require_arg(cmd, key);
    HandleState state;
    state.info = info;
    state.kind = kind;
    ctx.handles[handle_name] = state;
}

struct ParsedLine {
    Command command;
    bool valid = false;
};

ParsedLine parse_line(const std::string& line, int line_no) {
    ParsedLine parsed;
    size_t pos = 0;
    while(pos < line.size() && std::isspace(static_cast<unsigned char>(line[pos]))) {
        ++pos;
    }
    if(pos >= line.size() || line[pos] == '#') {
        return parsed;
    }
    size_t name_start = pos;
    while(pos < line.size() && !std::isspace(static_cast<unsigned char>(line[pos]))) {
        ++pos;
    }
    Command cmd;
    cmd.name = line.substr(name_start, pos - name_start);
    cmd.line = line_no;
    while(pos < line.size()) {
        while(pos < line.size() && std::isspace(static_cast<unsigned char>(line[pos]))) {
            ++pos;
        }
        if(pos >= line.size() || line[pos] == '#') {
            break;
        }
        size_t key_start = pos;
        while(pos < line.size() && line[pos] != '=' && !std::isspace(static_cast<unsigned char>(line[pos]))) {
            ++pos;
        }
        if(pos >= line.size() || line[pos] != '=') {
            throw std::runtime_error("expected '=' after key on line " + std::to_string(line_no));
        }
        std::string key = line.substr(key_start, pos - key_start);
        ++pos; // skip '='
        if(pos >= line.size()) {
            throw std::runtime_error("missing value for key '" + key + "' on line " + std::to_string(line_no));
        }
        std::string value;
        if(line[pos] == '"' || line[pos] == '\'') {
            char quote = line[pos++];
            while(pos < line.size()) {
                char c = line[pos++];
                if(c == quote) {
                    break;
                }
                if(c == '\\' && pos < line.size()) {
                    value.push_back('\\');
                    value.push_back(line[pos++]);
                } else {
                    value.push_back(c);
                }
            }
        } else {
            size_t value_start = pos;
            while(pos < line.size() && !std::isspace(static_cast<unsigned char>(line[pos]))) {
                ++pos;
            }
            value = line.substr(value_start, pos - value_start);
        }
        cmd.args.emplace(std::move(key), std::move(value));
    }
    parsed.command = std::move(cmd);
    parsed.valid = true;
    return parsed;
}

void ensure_mounted(const ExecutionContext& ctx, const Command& cmd) {
    if(!ctx.mounted) {
        std::ostringstream oss;
        oss << "command requires active mount";
        throw std::runtime_error(oss.str());
    }
}

void exec_backend_sql(ExecutionContext& ctx, const Command& cmd) {
    std::string sql = require_arg(cmd, "sql");
    auto expect_opt = optional_arg(cmd, "expect");

    if (cache_dir.empty()) {
        fail(ctx, cmd, "cache directory not set, MOUNT must be called first");
    }
    std::filesystem::path cache_path(cache_dir);
    std::filesystem::path db_path = cache_path / "cache.db";
    if (!std::filesystem::exists(db_path)) {
        if (expect_opt.has_value()) {
            fail(ctx, cmd, "cache.db does not exist in cache directory, but an expectation was provided.");
        }
        return;
    }

    sqlite3* db = nullptr;
    if (sqlite3_open(db_path.c_str(), &db) != SQLITE_OK) {
        std::string msg = "failed to open cache.db";
        if(db) {
            msg += ": " + std::string(sqlite3_errmsg(db));
            sqlite3_close(db);
        }
        fail(ctx, cmd, msg);
    }

    char* err_msg = nullptr;
    std::string query_result;

    auto callback = [](void* data, int argc, char** argv, char** azColName) -> int {
        std::string* result_str = static_cast<std::string*>(data);
        if (!result_str->empty()) {
            result_str->append("\n");
        }
        for (int i = 0; i < argc; i++) {
            if (i > 0) {
                result_str->append("|");
            }
            result_str->append(argv[i] ? argv[i] : "NULL");
        }
        return 0;
    };

    int rc = sqlite3_exec(db, sql.c_str(), callback, &query_result, &err_msg);

    if (rc != SQLITE_OK) {
        std::string error = "SQL error: " + std::string(err_msg);
        sqlite3_free(err_msg);
        sqlite3_close(db);
        fail(ctx, cmd, error);
    }

    sqlite3_close(db);

    if (expect_opt.has_value()) {
        if (query_result != *expect_opt) {
            std::ostringstream oss;
            oss << "SQL expect mismatch: wanted '" << *expect_opt << "', got '" << query_result << "'";
            fail(ctx, cmd, oss.str());
        }
    }
}

void run_backend_command(ExecutionContext& ctx, const Command& cmd) {
    if(cmd.name == "BACKEND_RESET") {
        backend_reset_state();
        return;
    }
    if(cmd.name == "BACKEND_CLEAR_CACHE") {
        backend_clear_cache_dir();
        cache_dir.clear();
        return;
    }
    if(cmd.name == "BACKEND_SEED") {
        std::string path = require_arg(cmd, "path");
        std::string data = decode_escapes(require_arg(cmd, "data"));
        backend_seed_file(path, data);
        return;
    }
    if(cmd.name == "BACKEND_EXPECT") {
        std::string path = require_arg(cmd, "path");
        std::optional<std::string> expected_opt = optional_arg(cmd, "data");
        std::optional<std::string> contains_opt = optional_arg(cmd, "contains");
        if(!expected_opt.has_value() && !contains_opt.has_value()) {
            fail(ctx, cmd, "BACKEND_EXPECT requires 'data=' or 'contains='");
        }
        std::string actual = backend_read_file(path);
        if(expected_opt.has_value()) {
            std::string expected = decode_escapes(*expected_opt);
            if(actual != expected) {
                std::ostringstream oss;
                oss << "backend content mismatch for '" << path << "'";
                fail(ctx, cmd, oss.str());
            }
        }
        if(contains_opt.has_value()) {
            std::string needle = decode_escapes(*contains_opt);
            if(actual.find(needle) == std::string::npos) {
                std::ostringstream oss;
                oss << "backend content missing substring '" << needle << "' at '" << path << "'";
                fail(ctx, cmd, oss.str());
            }
        }
        return;
    }
    if(cmd.name == "BACKEND_EXPECT_META") {
        std::string path = require_arg(cmd, "path");
        std::string meta_path = pathjoin(encodepath(path, file_encode_suffix), METANAME);
        if(!backend_path_exists(meta_path)) {
            fail(ctx, cmd, "meta not uploaded yet");
        }
        std::string meta = backend_read_file(meta_path);
        json_object* json = json_tokener_parse(meta.c_str());
        if(json == nullptr) {
            fail(ctx, cmd, "meta JSON parse failed");
        }
        for(const auto& entry : cmd.args) {
            if(entry.first == "path" || entry.first == "field" || entry.first == "value") {
                continue;
            }
            json_object_put(json);
            fail(ctx, cmd, "BACKEND_EXPECT_META only supports 'path', 'field', and 'value' arguments");
        }
        std::string field_path = decode_escapes(require_arg(cmd, "field"));
        std::string expected_value = decode_escapes(require_arg(cmd, "value"));
        try {
            json_object* node = resolve_json_path(json, field_path);
            if(node == nullptr) {
                throw std::runtime_error("resolved value was null");
            }
            std::string actual_value;
            json_type type = json_object_get_type(node);
            if(type == json_type_string) {
                actual_value = json_object_get_string(node);
            } else if(type == json_type_null) {
                actual_value = "null";
            } else {
                actual_value = json_object_to_json_string_ext(node, JSON_C_TO_STRING_PLAIN);
            }
            if(actual_value != expected_value) {
                std::ostringstream oss;
                oss << "expected '" << expected_value << "' but found '" << actual_value << "'";
                throw std::runtime_error(oss.str());
            }
        } catch(const std::exception& ex) {
            std::ostringstream oss;
            oss << "meta field '" << field_path << "' check failed: " << ex.what();
            json_object_put(json);
            fail(ctx, cmd, oss.str());
        }
        json_object_put(json);
        return;
    }
    if(cmd.name == "BACKEND_EXPECT_BLOCK") {
        std::string path = require_arg(cmd, "path");
        long block_index = parse_long(require_arg(cmd, "block"), 0);
        if(block_index < 0) {
            fail(ctx, cmd, "block index must be non-negative");
        }
        size_t offset = 0;
        if(auto offset_opt = optional_arg(cmd, "offset"); offset_opt.has_value()) {
            long off = parse_long(offset_opt.value(), 0);
            if(off < 0) {
                fail(ctx, cmd, "offset must be non-negative");
            }
            offset = static_cast<size_t>(off);
        }
        auto data_opt = optional_arg(cmd, "data");
        auto data_hex_opt = optional_arg(cmd, "data_hex");
        if(!data_opt.has_value() && !data_hex_opt.has_value()) {
            fail(ctx, cmd, "BACKEND_EXPECT_BLOCK requires 'data=' or 'data_hex='");
        }
        std::string encoded_dir = encodepath(path, file_encode_suffix);
        std::string meta_path = pathjoin(encoded_dir, METANAME);
        if(!backend_path_exists(meta_path)) {
            fail(ctx, cmd, "meta not uploaded yet");
        }
        std::string meta = backend_read_file(meta_path);
        json_object* json = json_tokener_parse(meta.c_str());
        if(json == nullptr) {
            fail(ctx, cmd, "meta JSON parse failed");
        }
        filemeta parsed_meta {};
        std::vector<filekey> file_blocks;
        if(unmarshal_meta(json, parsed_meta, file_blocks) != 0) {
            json_object_put(json);
            fail(ctx, cmd, "meta JSON unmarshal failed");
        }
        json_object_put(json);
        if(static_cast<size_t>(block_index) >= file_blocks.size()) {
            fail(ctx, cmd, "block index out of range");
        }
        std::string block_path;
        if(opt.flags & FM_RENAME_NOTSUPPRTED) {
            block_path = "/.objs/" + file_blocks[block_index].path;
        }else{
            block_path = pathjoin(encoded_dir, file_blocks[block_index].path);
        }
        if(!backend_path_exists(block_path)) {
            std::ostringstream oss;
            oss << "block not uploaded yet at '" << block_path << "'";
            fail(ctx, cmd, oss.str());
        }
        std::string raw_block = backend_read_file(block_path);
        if(raw_block.size() < offset) {
            fail(ctx, cmd, "block smaller than requested offset");
        }
        if(data_opt.has_value()) {
            std::string expected = decode_escapes(*data_opt);
            if(raw_block.size() < offset + expected.size()) {
                fail(ctx, cmd, "block smaller than expected data length");
            }
            std::string_view slice(raw_block.data() + offset, expected.size());
            if(slice != expected) {
                fail(ctx, cmd, "block data mismatch at provided offset");
            }
        } else {
            std::string expected_hex = *data_hex_opt;
            std::string lowered;
            lowered.reserve(expected_hex.size());
            for(char ch : expected_hex) {
                lowered.push_back(static_cast<char>(std::tolower(static_cast<unsigned char>(ch))));
            }
            if(lowered.size() % 2 != 0) {
                fail(ctx, cmd, "data_hex length must be even");
            }
            size_t bytes_needed = lowered.size() / 2;
            if(raw_block.size() < offset + bytes_needed) {
                fail(ctx, cmd, "block smaller than expected hex length");
            }
            std::string actual_hex;
            actual_hex.reserve(bytes_needed * 2);
            const unsigned char* begin = reinterpret_cast<const unsigned char*>(raw_block.data() + offset);
            for(size_t i = 0; i < bytes_needed; ++i) {
                unsigned char byte = begin[i];
                static const char hex_digits[] = "0123456789abcdef";
                actual_hex.push_back(hex_digits[byte >> 4]);
                actual_hex.push_back(hex_digits[byte & 0x0F]);
            }
            if(actual_hex != lowered) {
                fail(ctx, cmd, "block hex data mismatch at provided offset");
            }
        }
        return;
    }
    if(cmd.name == "BACKEND_SLEEP") {
        long ms = parse_long(require_arg(cmd, "ms"));
        if(ms < 0) {
            fail(ctx, cmd, "negative sleep duration");
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(ms));
        return;
    }
    if(cmd.name == "BACKEND_PATH_EXISTS") {
        std::string path = require_arg(cmd, "path");
        bool expected = parse_bool(require_arg(cmd, "expected"));
        bool actual = backend_path_exists(path);
        if (actual != expected) {
            fail(ctx, cmd, "expected path " + path + " existence to be " + (expected ? "true" : "false") + ", but got " + (actual ? "true" : "false"));
        }
        return;
    }
    if(cmd.name == "BACKEND_EXEC_SQL") {
        exec_backend_sql(ctx, cmd);
        return;
    }
    if(cmd.name == "BACKEND_CLONE") {
        std::string src = require_arg(cmd, "src");
        std::string dst = require_arg(cmd, "dst");
        backend_clone(src, dst);
        return;
    }

    fail(ctx, cmd, "unknown backend command");
}

int flags_from_string(const std::string& value) {
    if(value == "RDONLY") {
        return O_RDONLY;
    }
    if(value == "WRONLY") {
        return O_WRONLY;
    }
    if(value == "RDWR") {
        return O_RDWR;
    }
    if(value == "APPEND") {
        return O_APPEND;
    }
    if(value == "CREAT") {
        return O_CREAT;
    }
    if(value == "TRUNC") {
        return O_TRUNC;
    }
    if(value == "EXCL") {
        return O_EXCL;
    }
    throw std::runtime_error("unknown flag '" + value + "'");
}

int parse_flags(const Command& cmd) {
    auto value_opt = optional_arg(cmd, "flags");
    if(!value_opt.has_value()) {
        return 0;
    }
    const std::string& raw = value_opt.value();
    int flags = 0;
    std::stringstream ss(raw);
    std::string item;
    while(std::getline(ss, item, '|')) {
        if(item.empty()) {
            continue;
        }
        flags |= flags_from_string(item);
    }
    return flags;
}

mode_t parse_mode(const Command& cmd) {
    auto mode_opt = optional_arg(cmd, "mode");
    if(!mode_opt.has_value()) {
        return 0;
    }
    unsigned long value = parse_ulong(mode_opt.value(), 0);
    return static_cast<mode_t>(value);
}

void exec_mount(ExecutionContext& ctx, const Command& cmd) {
    if(ctx.mounted) {
        fail(ctx, cmd, "mount already active");
    }
    bool no_cache = false;
    bool rename_not_supported = false;
    if(auto opt_no_cache = optional_arg(cmd, "no_cache"); opt_no_cache.has_value()) {
        no_cache = parse_bool(opt_no_cache.value());
    }
    if(auto opt_rename = optional_arg(cmd, "rename_not_supported"); opt_rename.has_value()) {
        rename_not_supported = parse_bool(opt_rename.value());
    }
    uint flags = FM_DELETE_NEED_PURGE;
    if(rename_not_supported) {
        flags |= FM_RENAME_NOTSUPPRTED;
    }
    if(cache_dir.empty()) {
        cache_dir = std::filesystem::temp_directory_path() / "fmdisk_itest_XXXXXX";
        mkdtemp(cache_dir.data());
    }
    struct fmoption options {
        .cache_dir = cache_dir.c_str(),
        .secret = secret.c_str(),
        .block_len = 1 << 20,
        .flags = flags,
        .no_cache = no_cache ? 1 : 0,
        .cache_size = -1,
        .entry_cache_second = -1,
    };
    backend_set_mount_option(&options);
    int prep = fm_prepare();
    if(prep < 0) {
        fail(ctx, cmd, "fm_prepare failed with " + std::to_string(prep));
    }
    ctx.mount.userdata = fm_fuse_init(&ctx.mount.conn, &ctx.mount.cfg);
    if(ctx.mount.userdata == nullptr) {
        fail(ctx, cmd, "fm_fuse_init returned null");
    }
    ctx.mounted = true;
}

void exec_unmount(ExecutionContext& ctx, const Command& cmd) {
    if(!ctx.mounted) {
        fail(ctx, cmd, "no active mount to unmount");
    }
    fm_fuse_destroy(ctx.mount.userdata);
    ctx.mount.userdata = nullptr;
    ctx.mounted = false;
    ctx.handles.clear();
}

void validate_errno(int ret, const std::optional<int>& expected_errno, ExecutionContext& ctx, const Command& cmd, const char* step) {
    if(expected_errno.has_value()) {
        int want = -expected_errno.value();
        if(ret != want) {
            std::ostringstream oss;
            oss << step << " expected errno -" << expected_errno.value() << " got " << ret;
            fail(ctx, cmd, oss.str());
        }
        return;
    }
    if(ret < 0) {
        std::ostringstream oss;
        oss << step << " failed with " << ret;
        fail(ctx, cmd, oss.str());
    }
}

void exec_simple_path_call(ExecutionContext& ctx, const Command& cmd, const std::function<int(const char*)>& fn, const char* step_name) {
    ensure_mounted(ctx, cmd);
    std::string path = require_arg(cmd, "path");
    auto expected_errno = parse_expected_errno(cmd, "expect_error");
    int ret = fn(path.c_str());
    validate_errno(ret, expected_errno, ctx, cmd, step_name);
}

void exec_mkdir(ExecutionContext& ctx, const Command& cmd) {
    ensure_mounted(ctx, cmd);
    std::string path = require_arg(cmd, "path");
    mode_t mode = parse_mode(cmd);
    auto expected_errno = parse_expected_errno(cmd, "expect_error");
    int ret = fm_fuse_mkdir(path.c_str(), mode);
    validate_errno(ret, expected_errno, ctx, cmd, "mkdir");
}

void exec_unlink(ExecutionContext& ctx, const Command& cmd) {
    exec_simple_path_call(ctx, cmd, [](const char* path) { return fm_fuse_unlink(path); }, "unlink");
}

void exec_rmdir(ExecutionContext& ctx, const Command& cmd) {
    exec_simple_path_call(ctx, cmd, [](const char* path) { return fm_fuse_rmdir(path); }, "rmdir");
}

void exec_symlink(ExecutionContext& ctx, const Command& cmd) {
    ensure_mounted(ctx, cmd);
    std::string target = require_arg(cmd, "target");
    std::string link = require_arg(cmd, "link");
    auto expected_errno = parse_expected_errno(cmd, "expect_error");
    int ret = fm_fuse_symlink(target.c_str(), link.c_str());
    validate_errno(ret, expected_errno, ctx, cmd, "symlink");
}

void exec_readlink(ExecutionContext& ctx, const Command& cmd) {
    ensure_mounted(ctx, cmd);
    std::string path = require_arg(cmd, "path");
    size_t size = static_cast<size_t>(parse_ulong(require_arg(cmd, "size"), 0));
    auto expected_errno = parse_expected_errno(cmd, "expect_error");
    std::vector<char> buf(size);
    int ret = fm_fuse_readlink(path.c_str(), buf.data(), buf.size());
    if(expected_errno.has_value()) {
        validate_errno(ret, expected_errno, ctx, cmd, "readlink");
        return;
    }
    if(ret < 0) {
        fail(ctx, cmd, "readlink failed with " + std::to_string(ret));
    }
    std::string expected = decode_escapes(require_arg(cmd, "expect"));
    std::string actual;
    if(ret > 0) {
        actual.assign(buf.data(), buf.data() + ret);
    } else {
        actual.assign(buf.data());
    }
    if(expected != actual) {
        fail(ctx, cmd, "readlink expected '" + expected + "' got '" + actual + "'");
    }
}

void exec_statfs(ExecutionContext& ctx, const Command& cmd) {
    ensure_mounted(ctx, cmd);
    struct statvfs sf {};
    int ret = fm_fuse_statfs("/", &sf);
    if(ret < 0) {
        fail(ctx, cmd, "statfs failed with " + std::to_string(ret));
    }
    if(auto expected_block = optional_arg(cmd, "expect_block_size"); expected_block.has_value()) {
        unsigned long want = parse_ulong(expected_block.value(), 0);
        if(sf.f_bsize != want) {
            std::ostringstream oss;
            oss << "expected block size " << want << " got " << sf.f_bsize;
            fail(ctx, cmd, oss.str());
        }
    }
}

std::string type_string(mode_t mode) {
    if(S_ISREG(mode)) {
        return "file";
    }
    if(S_ISDIR(mode)) {
        return "dir";
    }
    if(S_ISLNK(mode)) {
        return "symlink";
    }
    return "other";
}

void exec_getattr(ExecutionContext& ctx, const Command& cmd) {
    ensure_mounted(ctx, cmd);
    HandleState* handle_state = nullptr;
    if(optional_arg(cmd, "handle").has_value()) {
        handle_state = &require_file_handle(ctx, cmd, "handle");
    }
    auto path = optional_arg(cmd, "path");
    struct stat st {};
    struct fuse_file_info* info_ptr = nullptr;
    if(handle_state != nullptr) {
        info_ptr = &handle_state->info;
    }
    auto expected_errno = parse_expected_errno(cmd, "expect_error");
    int ret = fm_fuse_getattr(path.has_value() ? path->c_str() : nullptr, &st, info_ptr);
    if(expected_errno.has_value()) {
        validate_errno(ret, expected_errno, ctx, cmd, "getattr");
        return;
    }
    if(ret < 0) {
        fail(ctx, cmd, "getattr failed with " + std::to_string(ret));
    }
    if(auto expect_type = optional_arg(cmd, "expect_type"); expect_type.has_value()) {
        std::string actual = type_string(st.st_mode);
        if(actual != expect_type.value()) {
            std::ostringstream oss;
            oss << "expected type '" << expect_type.value() << "' got '" << actual << "'";
            fail(ctx, cmd, oss.str());
        }
    }
    if(auto expect_size = optional_arg(cmd, "expect_size"); expect_size.has_value()) {
        long want = parse_long(expect_size.value(), 0);
        if(st.st_size != static_cast<off_t>(want)) {
            std::ostringstream oss;
            oss << "expected size " << want << " got " << st.st_size;
            fail(ctx, cmd, oss.str());
        }
    }
    if(auto expect_mode = optional_arg(cmd, "expect_mode"); expect_mode.has_value()) {
        mode_t want = static_cast<mode_t>(parse_ulong(expect_mode.value(), 0));
        mode_t actual = st.st_mode & 07777;
        if(actual != want) {
            std::ostringstream oss;
            oss << "expected mode " << std::oct << want << " got " << (actual);
            fail(ctx, cmd, oss.str());
        }
    }
    if(auto expect_mtime = optional_arg(cmd, "expect_mtime"); expect_mtime.has_value()) {
        long want = parse_long(expect_mtime.value(), 0);
        if(st.st_mtime != static_cast<time_t>(want)) {
            std::ostringstream oss;
            oss << "expected mtime " << want << " got " << st.st_mtime;
            fail(ctx, cmd, oss.str());
        }
    }
    if(auto expect_atime = optional_arg(cmd, "expect_atime"); expect_atime.has_value()) {
        long want = parse_long(expect_atime.value(), 0);
        if(st.st_atime != static_cast<time_t>(want)) {
            std::ostringstream oss;
            oss << "expected atime " << want << " got " << st.st_atime;
            fail(ctx, cmd, oss.str());
        }
    }
    if(auto expect_ctime = optional_arg(cmd, "expect_ctime"); expect_ctime.has_value()) {
        long want = parse_long(expect_ctime.value(), 0);
        if(st.st_ctime != static_cast<time_t>(want)) {
            std::ostringstream oss;
            oss << "expected ctime " << want << " got " << st.st_ctime;
            fail(ctx, cmd, oss.str());
        }
    }
}

void prepare_file_info_flags(fuse_file_info& info, const Command& cmd) {
    info.flags = parse_flags(cmd);
}

void exec_create(ExecutionContext& ctx, const Command& cmd) {
    ensure_mounted(ctx, cmd);
    std::string path = require_arg(cmd, "path");
    fuse_file_info info {};
    prepare_file_info_flags(info, cmd);
    mode_t mode = parse_mode(cmd);
    auto expected_errno = parse_expected_errno(cmd, "expect_error");
    int ret = fm_fuse_create(path.c_str(), mode, &info);
    if(expected_errno.has_value()) {
        validate_errno(ret, expected_errno, ctx, cmd, "create");
        if(ret >= 0) {
            fm_fuse_release(path.c_str(), &info);
        }
        return;
    }
    if(ret < 0) {
        fail(ctx, cmd, "create failed with " + std::to_string(ret));
    }
    store_handle(ctx, cmd, "handle", path, HandleKind::File, info);
}

void exec_open(ExecutionContext& ctx, const Command& cmd) {
    ensure_mounted(ctx, cmd);
    std::string path = require_arg(cmd, "path");
    fuse_file_info info {};
    prepare_file_info_flags(info, cmd);
    auto expected_errno = parse_expected_errno(cmd, "expect_error");
    int ret = fm_fuse_open(path.c_str(), &info);
    if(expected_errno.has_value()) {
        validate_errno(ret, expected_errno, ctx, cmd, "open");
        if(ret >= 0) {
            fm_fuse_release(path.c_str(), &info);
        }
        return;
    }
    if(ret < 0) {
        fail(ctx, cmd, "open failed with " + std::to_string(ret));
    }
    store_handle(ctx, cmd, "handle", path, HandleKind::File, info);
}

void exec_release(ExecutionContext& ctx, const Command& cmd) {
    ensure_mounted(ctx, cmd);
    std::string handle_name = require_arg(cmd, "handle");
    HandleState& state = require_file_handle(ctx, cmd, "handle");
    auto expected_errno = parse_expected_errno(cmd, "expect_error");
    bool sync = false;
    if(auto sync_arg = optional_arg(cmd, "sync"); sync_arg.has_value()){
        sync = parse_bool(sync_arg.value());
    }
    if(sync) {
        state.info.flags |= O_SYNC;
    }
    int ret = fm_fuse_release(nullptr, &state.info);
    validate_errno(ret, expected_errno, ctx, cmd, "release");
    if(ret >= 0) {
        ctx.handles.erase(handle_name);
    }
}

void exec_write(ExecutionContext& ctx, const Command& cmd) {
    ensure_mounted(ctx, cmd);
    HandleState& state = require_file_handle(ctx, cmd);
    std::string data_raw = require_arg(cmd, "data");
    std::string data = decode_escapes(data_raw);
    if(auto repeat_opt = optional_arg(cmd, "repeat"); repeat_opt.has_value()) {
        long repeat = parse_long(repeat_opt.value(), 0);
        if(repeat <= 0) {
            fail(ctx, cmd, "repeat must be positive");
        }
        if(data.empty()) {
            fail(ctx, cmd, "repeat requires non-empty data seed");
        }
        size_t repeat_size = static_cast<size_t>(repeat);
        if(repeat_size > 0 && data.size() > 0 && repeat_size > SIZE_MAX / data.size()) {
            fail(ctx, cmd, "repeat produces payload too large");
        }
        std::string expanded;
        expanded.reserve(data.size() * repeat_size);
        for(long i = 0; i < repeat; ++i) {
            expanded.append(data);
        }
        data = std::move(expanded);
    }
    long offset = 0;
    if(auto offset_opt = optional_arg(cmd, "offset"); offset_opt.has_value()) {
        offset = parse_long(offset_opt.value(), 0);
    }
    size_t size = data.size();
    if(auto size_opt = optional_arg(cmd, "size"); size_opt.has_value()) {
        long parsed_size = parse_long(size_opt.value(), 0);
        if(parsed_size < 0) {
            fail(ctx, cmd, "negative size not allowed");
        }
        size = static_cast<size_t>(parsed_size);
        if(size > data.size()) {
            data.resize(size, '\0');
        }
    }
    auto expected_errno = parse_expected_errno(cmd, "expect_error");
    int ret = fm_fuse_write(nullptr, data.data(), size, offset, &state.info);
    if(expected_errno.has_value()) {
        validate_errno(ret, expected_errno, ctx, cmd, "write");
        return;
    }
    if(ret < 0) {
        fail(ctx, cmd, "write failed with " + std::to_string(ret));
    }
    int expected_bytes = static_cast<int>(size);
    if(auto expect_opt = optional_arg(cmd, "expect_bytes"); expect_opt.has_value()) {
        expected_bytes = static_cast<int>(parse_long(expect_opt.value(), 0));
    }
    if(ret != expected_bytes) {
        std::ostringstream oss;
        oss << "write expected " << expected_bytes << " bytes got " << ret;
        fail(ctx, cmd, oss.str());
    }
}

void exec_read(ExecutionContext& ctx, const Command& cmd) {
    ensure_mounted(ctx, cmd);
    HandleState& state = require_file_handle(ctx, cmd);
    size_t size = static_cast<size_t>(parse_long(require_arg(cmd, "size"), 0));
    long offset = 0;
    if(auto offset_opt = optional_arg(cmd, "offset"); offset_opt.has_value()) {
        offset = parse_long(offset_opt.value(), 0);
    }
    std::vector<char> buffer(size);
    auto expected_errno = parse_expected_errno(cmd, "expect_error");
    int ret = fm_fuse_read(nullptr, buffer.data(), buffer.size(), offset, &state.info);
    if(expected_errno.has_value()) {
        validate_errno(ret, expected_errno, ctx, cmd, "read");
        return;
    }
    if(ret < 0) {
        fail(ctx, cmd, "read failed with " + std::to_string(ret));
    }
    if(auto expect_bytes = optional_arg(cmd, "expect_bytes"); expect_bytes.has_value()) {
        int want = static_cast<int>(parse_long(expect_bytes.value(), 0));
        if(ret != want) {
            std::ostringstream oss;
            oss << "read expected " << want << " bytes got " << ret;
            fail(ctx, cmd, oss.str());
        }
    }
    if(auto expect_data = optional_arg(cmd, "expect_data"); expect_data.has_value()) {
        std::string expected = decode_escapes(expect_data.value());
        std::string actual(buffer.data(), buffer.data() + ret);
        if(expected != actual) {
            fail(ctx, cmd, "read expected '" + expected + "' got '" + actual + "'");
        }
    }
}

void exec_truncate(ExecutionContext& ctx, const Command& cmd) {
    ensure_mounted(ctx, cmd);
    long length = parse_long(require_arg(cmd, "length"), 0);
    struct fuse_file_info* info_ptr = nullptr;
    HandleState* state_ptr = nullptr;
    if(optional_arg(cmd, "handle").has_value()) {
        state_ptr = &require_file_handle(ctx, cmd, "handle");
        info_ptr = &state_ptr->info;
    }
    auto path = optional_arg(cmd, "path");
    auto expected_errno = parse_expected_errno(cmd, "expect_error");
    int ret = fm_fuse_truncate(path.has_value() ? path->c_str() : nullptr, length, info_ptr);
    validate_errno(ret, expected_errno, ctx, cmd, "truncate");
}

void exec_rename(ExecutionContext& ctx, const Command& cmd) {
    ensure_mounted(ctx, cmd);
    std::string old_path = require_arg(cmd, "old");
    std::string new_path = require_arg(cmd, "new");
    unsigned int flags = 0;
    if(auto flags_opt = optional_arg(cmd, "flags"); flags_opt.has_value()) {
        flags = static_cast<unsigned int>(parse_ulong(flags_opt.value(), 0));
    }
    auto expected_errno = parse_expected_errno(cmd, "expect_error");
    int ret = fm_fuse_rename(old_path.c_str(), new_path.c_str(), flags);
    validate_errno(ret, expected_errno, ctx, cmd, "rename");
}

void exec_flush(ExecutionContext& ctx, const Command& cmd) {
    ensure_mounted(ctx, cmd);
    HandleState& state = require_file_handle(ctx, cmd);
    auto expected_errno = parse_expected_errno(cmd, "expect_error");
    int ret = fm_fuse_flush(nullptr, &state.info);
    validate_errno(ret, expected_errno, ctx, cmd, "flush");
}

void exec_fsync(ExecutionContext& ctx, const Command& cmd) {
    ensure_mounted(ctx, cmd);
    HandleState& state = require_file_handle(ctx, cmd);
    bool dataonly = false;
    if(auto data_opt = optional_arg(cmd, "data_only"); data_opt.has_value()) {
        dataonly = parse_bool(data_opt.value());
    }
    auto expected_errno = parse_expected_errno(cmd, "expect_error");
    int ret = fm_fuse_fsync(nullptr, dataonly ? 1 : 0, &state.info);
    validate_errno(ret, expected_errno, ctx, cmd, "fsync");
}

void exec_utimens(ExecutionContext& ctx, const Command& cmd) {
    ensure_mounted(ctx, cmd);
    struct timespec tv[2] {};
    tv[0].tv_sec = parse_long(require_arg(cmd, "atime_sec"), 0);
    tv[0].tv_nsec = parse_long(require_arg(cmd, "atime_nsec"), 0);
    tv[1].tv_sec = parse_long(require_arg(cmd, "mtime_sec"), 0);
    tv[1].tv_nsec = parse_long(require_arg(cmd, "mtime_nsec"), 0);
    struct fuse_file_info* info_ptr = nullptr;
    HandleState* state_ptr = nullptr;
    if(optional_arg(cmd, "handle").has_value()) {
        state_ptr = &require_file_handle(ctx, cmd, "handle");
        info_ptr = &state_ptr->info;
    }
    auto path = optional_arg(cmd, "path");
    auto expected_errno = parse_expected_errno(cmd, "expect_error");
    int ret = fm_fuse_utimens(path.has_value() ? path->c_str() : nullptr, tv, info_ptr);
    validate_errno(ret, expected_errno, ctx, cmd, "utimens");
}

void exec_chmod(ExecutionContext& ctx, const Command& cmd) {
    ensure_mounted(ctx, cmd);
    if(!cmd.args.contains("mode")) {
        fail(ctx, cmd, "CHMOD requires 'mode=' argument");
    }
    struct fuse_file_info* info_ptr = nullptr;
    if(optional_arg(cmd, "handle").has_value()) {
        HandleState& state = require_file_handle(ctx, cmd, "handle");
        info_ptr = &state.info;
    }
    auto path = optional_arg(cmd, "path");
    auto expected_errno = parse_expected_errno(cmd, "expect_error");
    mode_t mode = parse_mode(cmd);
    int ret = fm_fuse_chmod(path.has_value() ? path->c_str() : nullptr, mode, info_ptr);
    validate_errno(ret, expected_errno, ctx, cmd, "chmod");
}

void exec_setxattr(ExecutionContext& ctx, const Command& cmd) {
    ensure_mounted(ctx, cmd);
    std::string path = require_arg(cmd, "path");
    std::string name = require_arg(cmd, "name");
    auto value_opt = optional_arg(cmd, "value");
    std::string value_decoded;
    const char* value_ptr = nullptr;
    size_t size = 0;
    if(value_opt.has_value()) {
        value_decoded = decode_escapes(value_opt.value());
        value_ptr = value_decoded.c_str();
        size = value_decoded.size();
    }
    int flags = 0;
    if(auto flags_opt = optional_arg(cmd, "flags"); flags_opt.has_value()) {
        flags = static_cast<int>(parse_long(flags_opt.value(), 0));
    }
    auto expected_errno = parse_expected_errno(cmd, "expect_error");
    int ret = fm_fuse_setxattr(path.c_str(), name.c_str(), value_ptr, size, flags);
    validate_errno(ret, expected_errno, ctx, cmd, "setxattr");
}

void exec_command(ExecutionContext& ctx, const Command& cmd) {
    if(cmd.name.rfind("BACKEND_", 0) == 0) {
        run_backend_command(ctx, cmd);
        return;
    }
    if(cmd.name == "MOUNT") {
        exec_mount(ctx, cmd);
        return;
    }
    if(cmd.name == "UNMOUNT") {
        exec_unmount(ctx, cmd);
        return;
    }
    if(cmd.name == "STATFS") {
        exec_statfs(ctx, cmd);
        return;
    }
    if(cmd.name == "MKDIR") {
        exec_mkdir(ctx, cmd);
        return;
    }
    if(cmd.name == "UNLINK") {
        exec_unlink(ctx, cmd);
        return;
    }
    if(cmd.name == "RMDIR") {
        exec_rmdir(ctx, cmd);
        return;
    }
    if(cmd.name == "SYMLINK") {
        exec_symlink(ctx, cmd);
        return;
    }
    if(cmd.name == "READLINK") {
        exec_readlink(ctx, cmd);
        return;
    }
    if(cmd.name == "GETATTR") {
        exec_getattr(ctx, cmd);
        return;
    }
    if(cmd.name == "CREATE") {
        exec_create(ctx, cmd);
        return;
    }
    if(cmd.name == "OPEN") {
        exec_open(ctx, cmd);
        return;
    }
    if(cmd.name == "WRITE") {
        exec_write(ctx, cmd);
        return;
    }
    if(cmd.name == "READ") {
        exec_read(ctx, cmd);
        return;
    }
    if(cmd.name == "FLUSH") {
        exec_flush(ctx, cmd);
        return;
    }
    if(cmd.name == "RELEASE") {
        exec_release(ctx, cmd);
        return;
    }
    if(cmd.name == "TRUNCATE") {
        exec_truncate(ctx, cmd);
        return;
    }
    if(cmd.name == "RENAME") {
        exec_rename(ctx, cmd);
        return;
    }
    if(cmd.name == "FSYNC") {
        exec_fsync(ctx, cmd);
        return;
    }
    if(cmd.name == "UTIMENS") {
        exec_utimens(ctx, cmd);
        return;
    }
    if(cmd.name == "CHMOD") {
        exec_chmod(ctx, cmd);
        return;
    }
    if(cmd.name == "SETXATTR") {
        exec_setxattr(ctx, cmd);
        return;
    }
    fail(ctx, cmd, "unknown command");
}

std::vector<Command> load_script(const std::filesystem::path& path) {
    std::ifstream input(path);
    if(!input) {
        throw std::runtime_error("failed to open script " + path.string());
    }
    std::vector<Command> commands;
    std::string line;
    int line_no = 0;
    while(std::getline(input, line)) {
        ++line_no;
        ParsedLine parsed = parse_line(line, line_no);
        if(parsed.valid) {
            commands.push_back(std::move(parsed.command));
        }
    }
    return commands;
}

void run_script(const std::filesystem::path& path) {
    ExecutionContext ctx;
    ctx.script_name = path.string();
    auto commands = load_script(path);
    try {
        for(const auto& cmd : commands) {
            try {
                exec_command(ctx, cmd);
            } catch(const std::exception& inner) {
                std::string message = inner.what();
                std::string prefix = ctx.script_name + ":";
                if(message.find(prefix) != std::string::npos) {
                    throw;
                }
                fail(ctx, cmd, message);
            }
        }
        if(ctx.mounted) {
            fm_fuse_destroy(ctx.mount.userdata);
            ctx.mounted = false;
            ctx.handles.clear();
        }
    } catch(...) {
        if(ctx.mounted) {
            fm_fuse_destroy(ctx.mount.userdata);
            ctx.mounted = false;
        }
        ctx.handles.clear();
        throw;
    }
}

std::vector<std::filesystem::path> discover_scripts(const std::vector<std::string>& cli_paths) {
    std::vector<std::filesystem::path> scripts;
    if(!cli_paths.empty()) {
        for(const auto& item : cli_paths) {
            scripts.emplace_back(item);
        }
        return scripts;
    }
#ifdef FMDISK_TEST_SCRIPT_DIR
    const std::filesystem::path base_dir = std::filesystem::path(FMDISK_TEST_SCRIPT_DIR);
#else
    const std::filesystem::path base_dir = std::filesystem::path("scripts");
#endif
    if(!std::filesystem::exists(base_dir)) {
        throw std::runtime_error("script directory not found: " + base_dir.string());
    }
    for(const auto& entry : std::filesystem::directory_iterator(base_dir)) {
        if(entry.is_regular_file() && entry.path().extension() == ".fmtest") {
            scripts.push_back(entry.path());
        }
    }
    std::sort(scripts.begin(), scripts.end());
    return scripts;
}

int main(int argc, char** argv) {
    try {
        std::vector<std::string> cli_paths;
        for(int i = 1; i < argc; ++i) {
            cli_paths.emplace_back(argv[i]);
        }
        auto scripts = discover_scripts(cli_paths);
        if(scripts.empty()) {
            std::cerr << "no script files provided\n";
            return 1;
        }
        for(const auto& script_path : scripts) {
            run_script(script_path);
        }
        std::cout << "all fmdisk script scenarios passed\n";
        return 0;
    } catch(const std::exception& ex) {
        std::cerr << "script runner failed: " << ex.what() << "\n";
        return 1;
    }
}
