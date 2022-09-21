#ifndef __RAIZN_H__
#define __RAIZN_H__
#include <linux/bio.h>
#include <linux/device-mapper.h>
#include <linux/init.h>
#include <linux/kernel.h>
#include <linux/module.h>
#include <linux/raid/xor.h>
#include <uapi/linux/blkzoned.h>

// Modes:
// PROFILING - collect performance-related stats on a per-bio and per-stripe head basis
// DEBUG - PROFILING + verbose logging and request tracking

#define RAIZN_TEST

#define RAIZN_MD_MAGIC 42

#define RAIZN_MD_SUPERBLOCK 0
#define RAIZN_MD_RESET_LOG 1
#define RAIZN_MD_PARITY_LOG 2

#ifdef  DEBUG
#define DEBUG_PRINT 1
#define PROFILING
#else
#define DEBUG_PRINT 0
#endif
#define printd(fmt, ...) \
						do { if (DEBUG_PRINT) pr_info(fmt, ##__VA_ARGS__); } while (0)

#define profile_bio(sh) \
	do {if (PROFILING) raizn_record_op(sh); } while (0)

#define RAIZN_MAX_DEVS (8)
#define RAIZN_BIO_POOL_SIZE (512)
#define RAIZN_MAX_ZONES (8192)
#define RAIZN_WQ_MAX_DEPTH (8192)

#define WQ_NAME "RAIZN_WQ"
#define SCACHE_NAME "raizn-stripe-cache"
// ctx params
#define NUM_TABLE_PARAMS (4)
#define MIN_DEVS 			 (3)
#define RAIZN_RESERVED_ZONES (5)
#define NUM_PARITY_DEV	 (1)
#define RAIZN_MAX_SUB_IOS (256)
#define RAIZN_MAX_BVECS (BIO_MAX_VECS)

#define STRIPE_BUFFERS_PER_ZONE (4)
#define STRIPE_BUFFERS_MASK (STRIPE_BUFFERS_PER_ZONE - 1)

#define RAIZN_GEN_COUNTERS_PER_PAGE (PAGE_SIZE / (sizeof(uint64_t) * 2) - 1)

#define RAIZN_DEV_TOGGLE_CMD "dev_toggle"
#define RAIZN_DEV_REBUILD_CMD "dev_rebuild"

// Params that are constant across all devices
struct raizn_params {
	/* Number of drives in the array */
	int array_width;
	/* Number of data drives in the array */
	int stripe_width;
	/* Number of zones in the array */
	int num_zones;
	/* Zone size and capacity (sectors) of a logical zone */
	sector_t lzone_size_sectors;
	sector_t lzone_capacity_sectors;
	/* Number of sectors in a stripe unit */
	sector_t su_sectors;
	/* Number of sectors in a stripe
	   Keep this separate from su_sectors so we can configure encoding*/
	sector_t stripe_sectors;
	int lzone_shift, su_shift;
};

// Per-device superblock
struct __attribute__((__packed__)) raizn_superblock {
	int idx; // index of this disk in the array
	struct raizn_params params;
	char padding[PAGE_SIZE - (sizeof(struct raizn_params) - sizeof(int))];
};

typedef enum raizn_zone_type {
	RAIZN_ZONE_MD_GENERAL = 0,
	RAIZN_ZONE_MD_PARITY_LOG,
	RAIZN_ZONE_NUM_MD_TYPES,
	RAIZN_ZONE_DATA,
	RAIZN_ZONE_NUM_TYPES
} raizn_zone_type;

struct raizn_workqueue {
	struct raizn_ctx *ctx;
	struct raizn_dev *dev;
	int num_threads;
	DECLARE_KFIFO_PTR(work_fifo, struct raizn_stripe_head *);
	spinlock_t wlock, rlock;
	struct work_struct work;
};

struct raizn_stripe_buffer {
	char *data;
	sector_t lba;
	struct mutex lock;
};

// Reuse same struct for logical, physical, and metadata zone descriptors
struct raizn_zone {
	struct mutex lock;
	volatile sector_t wp;
	atomic_t cond;
	sector_t start;
	sector_t capacity;
	sector_t len;
	struct raizn_stripe_buffer *stripe_buffers;
	union {
		unsigned long *persistence_bitmap;
		struct raizn_dev *dev;
	};
	volatile raizn_zone_type zone_type;
	atomic_t refcount;
	// TODO: add a list of stripe heads
	// Zone reset after refcount reaches 0 -> add stripe head to list and endio will check on completion of each IO
};

/*
 * stores information about underlying devices for single RAIZN logical device
 */
struct raizn_dev {
	struct dm_dev *dev;
	unsigned int num_zones;
	struct bio_set bioset; // for allocating RAIZN-specific bios for this device
	struct mutex lock, bioset_lock;
	struct raizn_zone *zones;
	spinlock_t free_rlock, free_wlock;
	DECLARE_KFIFO_PTR(free_zone_fifo, struct raizn_zone *);
	struct raizn_zone *md_zone[RAIZN_ZONE_NUM_MD_TYPES];
	struct raizn_workqueue gc_ingest_workers;
	struct raizn_workqueue gc_flush_workers;
	int zone_shift;
	int idx;
	struct raizn_superblock sb;
};


struct __attribute__((__packed__)) raizn_md_header {
	struct __attribute__((__packed__)) raizn_md_header_header {
		uint32_t magic, logtype;
		sector_t start, end;
		uint64_t zone_generation;
	} header;
	char data[PAGE_SIZE - sizeof(struct raizn_md_header_header)];
};

struct raizn_rebuild_mgr {
	sector_t rp, wp; // logical read pointer, physical write pointer
	struct raizn_dev *target_dev;
	unsigned long *open_zones; // Bitmap of open zones that haven't been rebuilt yet
	unsigned long *incomplete_zones; // Bitmap of non-open zones that haven't been rebuilt yet
	struct mutex lock;
	ktime_t start, end;
};

// Zone manager data
struct raizn_zone_mgr {
	struct raizn_zone *lzones;
	struct raizn_rebuild_mgr rebuild_mgr;
	struct __attribute__((__packed__)) {
		// Each generation counter contains a 64-bit global counter and 511 64-bit local counters
		uint64_t magic;
		uint64_t global_generation;
		uint64_t zone_generation[PAGE_SIZE / (sizeof(uint64_t) * 2) - 1];
	} *gen_counts;
};

// Main context for raizn
struct raizn_ctx {
	// device list
	struct raizn_dev *devs;
	DECLARE_BITMAP(dev_status, RAIZN_MAX_DEVS);
	struct raizn_params *params;
	struct raizn_zone_mgr zone_mgr;
	struct bio_set bioset; // For cloning/splitting bios that are never submitted
	struct raizn_workqueue io_workers;
	int num_io_workers, num_gc_workers;

#ifdef PROFILING
	struct {
		atomic64_t write_sectors, read_sectors;
		atomic_t writes, reads, zone_resets, flushes;
		atomic_t preflush, fua;
		atomic_t gc_count;
	} counters;
#endif
};

typedef enum {
	RAIZN_OP_OTHER = 0,
	RAIZN_OP_GC,
	RAIZN_OP_REBUILD_INGEST,
	RAIZN_OP_REBUILD_FLUSH,
	RAIZN_OP_DEGRADED_READ,
	RAIZN_OP_ZONE_RESET_LOG,
	RAIZN_OP_READ, // Start 1:1 mapping with REQ_OP_*
	RAIZN_OP_WRITE,
	RAIZN_OP_FLUSH,
	RAIZN_OP_DISCARD,
	RAIZN_OP_SECURE_ERASE,
	RAIZN_OP_WRITE_ZEROES,
	RAIZN_OP_ZONE_OPEN,
	RAIZN_OP_ZONE_CLOSE,
	RAIZN_OP_ZONE_FINISH,
	RAIZN_OP_ZONE_APPEND,
	RAIZN_OP_ZONE_RESET,
	RAIZN_OP_ZONE_RESET_ALL
} raizn_op_t;

typedef enum {
	RAIZN_SUBIO_DATA = 0,
	RAIZN_SUBIO_PARITY,
	RAIZN_SUBIO_MD,
	RAIZN_SUBIO_REBUILD,
	RAIZN_SUBIO_OTHER
} sub_io_type_t;

struct raizn_sub_io {
	struct raizn_md_header header;
	struct bio *bio;
	sub_io_type_t sub_io_type;
	void *data;
	struct raizn_zone *zone;
	bool defer_put;
	struct raizn_stripe_head *sh;
	sector_t dbg;
};

// raizn_stripe_head describes all of the information
// necessary to execute or resume a raizn operation
struct raizn_stripe_head {
	struct raizn_ctx *ctx;
	struct bio *orig_bio; // Original bio submitted by upper layer
	raizn_op_t op;

	enum {
		RAIZN_IO_CREATED = 0,
		RAIZN_IO_COMPLETED,
		RAIZN_IO_FAILED
	} status;
	struct raizn_sub_io *sub_ios[RAIZN_MAX_SUB_IOS];
	struct raizn_sub_io sentinel;
	// Handling for writes
	struct bio *bios[RAIZN_MAX_DEVS];
	uint8_t *parity_bufs;

	// Handling for garbage collection or rebuild
	sector_t lba; // TODO refactor this into zone ptr
	struct raizn_zone *zone;
	struct raizn_rebuild_buffer *rebuild_buf;

	// Chain dependencies
	struct raizn_stripe_head *next;

	atomic_t subio_idx;
	atomic_t refcount;
};

#endif //ifdef __RAIZN_H__
