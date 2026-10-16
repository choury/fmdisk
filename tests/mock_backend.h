#pragma once

#include "utils.h"

#include <string>

void backend_note_download();
long long backend_download_calls();
long long backend_restore_calls();
int backend_last_restore_days();
void backend_set_storage(const std::string& path, enum storage_class storage);
void backend_set_restoring(const std::string& path);
void backend_set_fail_upload(bool fail);
void backend_set_upload_delay(int ms);

void backend_reset_state();
void backend_seed_file(const std::string& path, const std::string& content);
void backend_unlink(const std::string& path);
void backend_set_mount_option(const struct fmoption* opt);
void backend_clear_cache_dir();
bool backend_path_exists(const std::string& path);
std::string backend_read_file(const std::string& path);
void backend_clone(const std::string& src, const std::string& dst);
