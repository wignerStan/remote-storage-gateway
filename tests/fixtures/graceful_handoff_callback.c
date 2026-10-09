#define _POSIX_C_SOURCE 200809L
#include "infra_plugin_v1.h"
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

/* Each test compiles its own library with private marker and release paths.
 * The held call deliberately ignores cooperative cancellation. */
static void mark(const char *event) {
    char line[80];
    int length = snprintf(line, sizeof(line), "%s %ld\n", event, (long)getpid());
    int fd = open(TEST_MARKERS, O_WRONLY | O_CREAT | O_APPEND, 0600);
    if (fd < 0 || write(fd, line, (size_t)length) != length) abort();
    if (close(fd)) abort();
}
static uint32_t init(const InfraHostV1 *host, uint64_t *state) {
    (void)host; (void)state;
#ifdef TEST_FAIL_INIT
    mark("init_failed");
    return INFRA_FAILED;
#else
    *state = 1;
    mark("init");
    return INFRA_OK;
#endif
}
static uint32_t open_callback(uint64_t state, uint64_t binding, InfraBytes input, uint64_t *handle) {
    (void)state; (void)input;
    if (binding) return INFRA_FAILED;
    *handle = 1;
    mark("open");
    return INFRA_OK;
}
static uint32_t call(uint64_t state, uint64_t handle, InfraBytes method, InfraBytes input,
        InfraCallMeta meta, InfraOwnedBytes *out) {
    (void)state; (void)handle; (void)method; (void)meta;
    const char needle[] = "\"path\":\"/native-held\"";
    int held = 0;
    for (size_t i = 0; i + sizeof(needle) - 1 <= input.len; i++) {
        if (!memcmp(input.ptr + i, needle, sizeof(needle) - 1)) { held = 1; break; }
    }
    if (held) {
        mark("enter");
        struct timespec pause = {0, 10000000};
        while (access(TEST_RELEASE, F_OK)) nanosleep(&pause, NULL);
        /* Read borrowed input after the wait to detect lost ownership. */
        if (input.len < 2 || input.ptr[0] != '{' || input.ptr[input.len - 1] != '}') abort();
    }
    const char reply[] = "{\"version\":1,\"decision\":\"continue\"}";
    out->len = sizeof(reply) - 1;
    out->ptr = malloc(out->len + 1);
    if (!out->ptr) abort();
    memcpy(out->ptr, reply, out->len);
    out->ptr[out->len] = (uint8_t)held;
    if (held) mark("return");
    return INFRA_OK;
}
static uint32_t next(uint64_t state, uint64_t handle, uint32_t credit, InfraCallMeta meta,
        InfraOwnedBytes *out, uint32_t *eof) {
    (void)state; (void)handle; (void)credit; (void)meta;
    *out = (InfraOwnedBytes){0}; *eof = 1; return INFRA_OK;
}
static uint32_t close_callback(uint64_t state, uint64_t handle) {
    (void)state; (void)handle; mark("close"); return INFRA_OK;
}
static uint32_t destroy(uint64_t state) { (void)state; mark("destroy"); return INFRA_OK; }
static void free_reply(uint64_t state, InfraOwnedBytes reply) {
    (void)state;
    if (reply.ptr[reply.len]) mark("free");
    free(reply.ptr);
}
static const InfraPluginV1 table = {
    { sizeof(InfraPluginV1), INFRA_PLUGIN_ABI_VERSION, INFRA_PLUGIN_FUNCTION,
      INFRA_LANGUAGE_RUST, INFRA_UNLOAD_EXPLICIT,
      { (const uint8_t *)"worker.handoff.test", 19 }, { (const uint8_t *)"native", 6 } },
    init, open_callback, call, next, close_callback, destroy, free_reply
};
const InfraPluginV1 *infra_plugin_v1_entry(void) { return &table; }
