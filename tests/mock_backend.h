#pragma once

#include <string>

void backend_note_download();
long long backend_download_calls();
void backend_set_fail_upload(bool fail);

void backend_reset_state();
void backend_seed_file(const std::string& path, const std::string& content);
void backend_set_mount_option(const struct fmoption* opt);
void backend_clear_cache_dir();
bool backend_path_exists(const std::string& path);
std::string backend_read_file(const std::string& path);
void backend_clone(const std::string& src, const std::string& dst);
