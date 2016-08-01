
/*
 * pdlfs_config.h  configuration and options from the build system
 * 02-Jun-2016  chuck@ece.cmu.edu
 */

#ifndef PDLFS_COMMON_PDLFS_CONFIG_H
#define PDLFS_COMMON_PDLFS_CONFIG_H 1

/*
 * this file abstracts out building with old-style makefiles and cmake
 * builds.   when we stop using old-style makefiles we can just move
 * pdlfs_config_expand.h here.
 */
#if defined(PDLFS_MAKEFILE)

/*
 * old-style makefile must pass platform information in via -D on
 * command line, so we don't do anything here.
 */

#else /* PDLFS_MAKEFILE */

/*
 * pull in the correct file expanded by cmake...  the file name
 * depends on what PDLFS_COMMON_LIBNAME we were compiled under.
 * we expect that to be provided via PDLFS_CONFIG and use some
 * preprocessor magic to turn it into the correct #include...
 */
#ifndef PDLFS_CONFIG
#define PDLFS_CONFIG pdlfs-common       /* the default name */
#endif

#define PDLFS_cat(X,Y) X ## Y          /* concat */
#define PDLFS_str(X) PDLFS_hstr(X)     /* expand "X" before stringifying it */
#define PDLFS_hstr(X) #X               /* stringify X */
#define PDLFS_fn(X) pdlfs-common/PDLFS_cat(X, _config_expand.h)

#include PDLFS_str(PDLFS_fn(PDLFS_CONFIG))

#undef PDLFS_cat
#undef PDLFS_str
#undef PDLFS_hstr
#undef PDLFS_fn

#endif /* PDLFS_MAKEFILE */

#endif /* PDLFS_COMMON_PDLFS_CONFIG_H */
