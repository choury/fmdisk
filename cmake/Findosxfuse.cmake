# This module can find FUSE Library
#
# The following variables will be defined for your use:
# - FUSE_FOUND : was FUSE found?
# - FUSE_INCLUDE_DIRS : FUSE include directory
# - FUSE_LIBRARIES : FUSE library

find_path(FUSE_INCLUDE_DIRS
    NAMES fuse_common.h fuse_lowlevel.h fuse.h)

find_library(FUSE_LIBRARIES NAMES osxfuse)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(osxfuse DEFAULT_MSG FUSE_INCLUDE_DIRS FUSE_LIBRARIES)

mark_as_advanced(FUSE_INCLUDE_DIRS FUSE_LIBRARIES)
