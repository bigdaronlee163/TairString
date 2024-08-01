#include "redismodule.h"
#include <stdlib.h>

int MathDouble_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    long long num;

    if (argc == 2) {
        RedisModule_StringToLongLong(argv[1], &num);
        RedisModule_ReplyWithLongLong(ctx, num * num);
        return REDISMODULE_OK;
    }

    return RedisModule_ReplyWithError(ctx, "ERR invalid num parameters");
}

int RedisModule_OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (RedisModule_Init(ctx, "math", 1, REDISMODULE_APIVER_1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx, "math.double",
            MathDouble_RedisCommand, "readonly", 1, 1, 1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    return REDISMODULE_OK;
}