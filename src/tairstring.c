/*
 * Copyright 2021 Alibaba Tair Team
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "tairstring.h"

#include <assert.h>
#include <ctype.h>
#include <errno.h>
#include <limits.h>
#include <math.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>

#include "redismodule.h"
#include "util.h"
// 没有额外的参数。 不存在  存在   过期时间  
// 版本。
#define TAIR_STRING_SET_NO_FLAGS 0
#define TAIR_STRING_SET_NX (1 << 0)
#define TAIR_STRING_SET_XX (1 << 1)
#define TAIR_STRING_SET_EX (1 << 2)
#define TAIR_STRING_SET_PX (1 << 3)
#define TAIR_STRING_SET_ABS_EXPIRE (1 << 4)
#define TAIR_STRING_SET_WITH_VER (1 << 5)
#define TAIR_STRING_SET_WITH_ABS_VER (1 << 6)
#define TAIR_STRING_SET_WITH_BOUNDARY (1 << 7)
#define TAIR_STRING_SET_WITH_FLAGS (1 << 8)
#define TAIR_STRING_SET_WITH_DEF (1 << 9)
#define TAIR_STRING_SET_NONEGATIVE (1 << 10)
#define TAIR_STRING_RETURN_WITH_VER (1 << 11)
#define TAIR_STRING_SET_KEEPTTL (1 << 12)

#define TAIRSTRING_ENCVER_VER_1 0

static RedisModuleType *TairStringType;
// 代码中的#pragma pack(1)是一个编译指令，用来指定结构体成员变量的对齐方式为1字节，即按照最小对齐原则进行对齐。这样可以确保结构体在内存中的布局是紧凑的，节省内存空间。
#pragma pack(1)
typedef struct TairStringObj {
    uint64_t version;
    uint32_t flags;
    RedisModuleString *value;
} TairStringObj;

// 分配和释放内存的函数。
static struct TairStringObj *createTairStringTypeObject(void) {
    return (TairStringObj *)RedisModule_Calloc(1, sizeof(TairStringObj));
}

static void TairStringTypeReleaseObject(struct TairStringObj *o) {
    if (!o) return;

    if (o->value) {
        RedisModule_FreeString(NULL, o->value);
    }

    RedisModule_Free(o);
}
// 转成long double类型的。
static int mstring2ld(RedisModuleString *val, long double *r_val) {
    if (!val) return REDISMODULE_ERR;

    size_t t_len;
    const char *t_ptr = RedisModule_StringPtrLen(val, &t_len);
    if (m_string2ld(t_ptr, t_len, r_val) == 0) {
        return REDISMODULE_ERR;
    }

    return REDISMODULE_OK;
}

static int mstringcasecmp(const RedisModuleString *rs1, const char *s2) {
    size_t n1 = strlen(s2);
    size_t n2;
    const char *s1 = RedisModule_StringPtrLen(rs1, &n2);
    if (n1 != n2) {
        return -1;
    }
    return strncasecmp(s1, s2, n1);
}

/* Parse the command **argv and get those arguments. Return ex_flags. If parsing
 * get failed, It would reply with syntax error. The first appearance would be
 * accepted if there are multiple appearance of a same group, For example: "EX
 * 3 PX 4000 EXAT 127", the "EX 3" would be accepted and the other two would be
 * ignored.
 * */
/*
1. 这个函数 parseAndGetExFlags 是一个静态函数，用于解析Redis命令参数并获取相关的标志（flags）。如果解析失败，它会返回语法错误。
    argv：命令参数数组。
    argc：参数数量。
    start：解析开始的索引。
    ex_flag：指向标志的指针，用于存储解析后的标志。
    expire_p、version_p、flags_p、defaultvalue_p、min_p、max_p：指向不同参数的指针，用于存储相应的参数值。
    allow_flags：允许的标志，用于验证解析后的标志是否合法。
2. 这个函数的主要功能是解析传入的参数数组，并根据参数设置不同的标志。它确保标志的合法性，并处理冲突的标志。在解析过程中，如果发现任何语法错误或冲突，它会立即返回错误码 REDISMODULE_ERR。
通过这种方式，函数能够有效地解析命令参数，并为后续的命令处理提供所需的标志和参数值。

*/
static int parseAndGetExFlags(RedisModuleString **argv, int argc, int start, int *ex_flag, RedisModuleString **expire_p,
                              RedisModuleString **version_p, RedisModuleString **flags_p,
                              RedisModuleString **defaultvalue_p, RedisModuleString **min_p,
                              RedisModuleString **max_p, unsigned int allow_flags) {
    // TAIR_STRING_SET_NO_FLAGS 初始值是0，然后如果存在某个标志位，将其和对应位置相与。
    int j, ex_flags = TAIR_STRING_SET_NO_FLAGS;
    for (j = start; j < argc; j++) {
        RedisModuleString *next = (j == argc - 1) ? NULL : argv[j + 1];

        if (!mstringcasecmp(argv[j], "nx")) {
            // 如果没有设置 则才会进行设置？ 为什么要进行这个判断？ 
            // 避免exset中重复的参数。【for循环，遍历 EXFalgs 的左右参数。EXFalgs 】
            if (ex_flags & TAIR_STRING_SET_XX) {
                return REDISMODULE_ERR;
            }
            ex_flags |= TAIR_STRING_SET_NX;
        } else if (!mstringcasecmp(argv[j], "xx")) {
            if (ex_flags & TAIR_STRING_SET_NX) {
                return REDISMODULE_ERR;
            }
            ex_flags |= TAIR_STRING_SET_XX;
        } else if (expire_p != NULL && !mstringcasecmp(argv[j], "ex") && next) {
            if (ex_flags & (TAIR_STRING_SET_PX | TAIR_STRING_SET_EX | TAIR_STRING_SET_KEEPTTL)) {
                return REDISMODULE_ERR;
            }
            ex_flags |= TAIR_STRING_SET_EX;
            *expire_p = next;
            j++;
        } else if (expire_p != NULL && !mstringcasecmp(argv[j], "exat") && next) {
            if (ex_flags & (TAIR_STRING_SET_PX | TAIR_STRING_SET_EX | TAIR_STRING_SET_KEEPTTL)) {
                return REDISMODULE_ERR;
            }
            ex_flags |= TAIR_STRING_SET_EX;
            ex_flags |= TAIR_STRING_SET_ABS_EXPIRE;
            *expire_p = next;
            j++;
        } else if (expire_p != NULL && !mstringcasecmp(argv[j], "px") && next) {
            if (ex_flags & (TAIR_STRING_SET_PX | TAIR_STRING_SET_EX | TAIR_STRING_SET_KEEPTTL)) {
                return REDISMODULE_ERR;
            }
            ex_flags |= TAIR_STRING_SET_PX;
            *expire_p = next;
            j++;
        } else if (expire_p != NULL && !mstringcasecmp(argv[j], "pxat") && next) {
            if (ex_flags & (TAIR_STRING_SET_PX | TAIR_STRING_SET_EX | TAIR_STRING_SET_KEEPTTL)) {
                return REDISMODULE_ERR;
            }
            ex_flags |= TAIR_STRING_SET_PX;
            ex_flags |= TAIR_STRING_SET_ABS_EXPIRE;
            // 时间
            *expire_p = next;
            j++;
        } else if (version_p != NULL && !mstringcasecmp(argv[j], "ver") && next) {
            if (ex_flags & (TAIR_STRING_SET_WITH_VER | TAIR_STRING_SET_WITH_ABS_VER)) {
                return REDISMODULE_ERR;
            }
            ex_flags |= TAIR_STRING_SET_WITH_VER;
            // version 
            *version_p = next;
            j++;
        } else if (version_p != NULL && !mstringcasecmp(argv[j], "abs") && next) {
            if (ex_flags & (TAIR_STRING_SET_WITH_VER | TAIR_STRING_SET_WITH_ABS_VER)) {
                return REDISMODULE_ERR;
            }
            ex_flags |= TAIR_STRING_SET_WITH_ABS_VER;
            *version_p = next;
            j++;
        } else if (flags_p != NULL && !mstringcasecmp(argv[j], "flags") && next) {
            if (ex_flags & TAIR_STRING_SET_WITH_FLAGS) {
                return REDISMODULE_ERR;
            }
            ex_flags |= TAIR_STRING_SET_WITH_FLAGS;
            *flags_p = next;
            j++;
        } else if (defaultvalue_p != NULL && !mstringcasecmp(argv[j], "def") && next) { /* DEF disabled if XX set. */
            if (ex_flags & TAIR_STRING_SET_WITH_DEF) {
                return REDISMODULE_ERR;
            }
            ex_flags |= TAIR_STRING_SET_WITH_DEF;
            *defaultvalue_p = next;
            j++;
        } else if (min_p != NULL && !mstringcasecmp(argv[j], "min") && next) {
            if (*min_p != NULL) {
                return REDISMODULE_ERR;
            }
            ex_flags |= TAIR_STRING_SET_WITH_BOUNDARY;
            *min_p = next;
            j++;
        } else if (max_p != NULL && !mstringcasecmp(argv[j], "max") && next) {
            if (*max_p != NULL) {
                return REDISMODULE_ERR;
            }
            ex_flags |= TAIR_STRING_SET_WITH_BOUNDARY;
            *max_p = next;
            j++;
        } else if (!mstringcasecmp(argv[j], "nonegative")) {
            ex_flags |= TAIR_STRING_SET_NONEGATIVE;
        } else if (!mstringcasecmp(argv[j], "withversion")) {
            ex_flags |= TAIR_STRING_RETURN_WITH_VER;
        } else if (!mstringcasecmp(argv[j], "keepttl")) {
            // 不能和 nx xx 一起使用。
            if (ex_flags & (TAIR_STRING_SET_PX | TAIR_STRING_SET_EX)) {
                return REDISMODULE_ERR;
            }
            ex_flags |= TAIR_STRING_SET_KEEPTTL;
        } else {
            return REDISMODULE_ERR;
        }
    }
    
if ((~allow_flags) & ex_flags) {
        return REDISMODULE_ERR;
    }

    *ex_flag = ex_flags;
    return REDISMODULE_OK;
}

/* ========================= "tairstring" type commands =======================*/
// 官方文档里面，都没有  [FLAGS flags] [WITHVERSION]。
// flags 应该就是 nonegative  withversion （exget 默认返回版本信息。）
/* EXSET <key> <value> [EX/EXAT/PX/PXAT time] [NX/XX] [VER/ABS version] [FLAGS flags] [WITHVERSION] [KEEPTTL] */
int TairStringTypeSet_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);
    // 至少需要三个参数。
    if (argc < 3) {
        return RedisModule_WrongArity(ctx);
    }

    long long milliseconds = 0, expire = 0, version = 0, flags = 0;
    RedisModuleString *expire_p = NULL, *version_p = NULL, *flags_p = NULL;
    int ex_flags = TAIR_STRING_SET_NO_FLAGS;
    // 支持的参数。
    unsigned int allow_flags = TAIR_STRING_SET_NX | TAIR_STRING_SET_XX | TAIR_STRING_SET_EX | TAIR_STRING_SET_PX | 
                      TAIR_STRING_SET_ABS_EXPIRE | TAIR_STRING_SET_KEEPTTL | TAIR_STRING_SET_WITH_VER |
                      TAIR_STRING_SET_WITH_ABS_VER | TAIR_STRING_SET_WITH_FLAGS | TAIR_STRING_RETURN_WITH_VER;
    // 参数的起始位置是3 （不是从0开始的吗？ ）
    if (parseAndGetExFlags(argv, argc, 3, &ex_flags, &expire_p, &version_p, &flags_p, NULL, NULL, NULL, allow_flags) != REDISMODULE_OK) {
        // 参数解析失败。 ERR syntax error
        RedisModule_ReplyWithError(ctx, TAIRSTRING_ERRORMSG_SYNTAX);
        return REDISMODULE_ERR;
    }

    if ((NULL != expire_p) && (RedisModule_StringToLongLong(expire_p, &expire) != REDISMODULE_OK)) {
        RedisModule_ReplyWithError(ctx, TAIRSTRING_ERRORMSG_SYNTAX);
        return REDISMODULE_ERR;
    }

    if ((NULL != version_p) && (RedisModule_StringToLongLong(version_p, &version) != REDISMODULE_OK)) {
        RedisModule_ReplyWithError(ctx, TAIRSTRING_ERRORMSG_SYNTAX);
        return REDISMODULE_ERR;
    }

    if ((NULL != flags_p) && (RedisModule_StringToLongLong(flags_p, &flags) != REDISMODULE_OK)) {
        RedisModule_ReplyWithError(ctx, TAIRSTRING_ERRORMSG_SYNTAX);
        return REDISMODULE_ERR;
    }

    if ((expire_p && expire <=0) || version < 0 || flags < 0 || flags > UINT_MAX) {
        RedisModule_ReplyWithError(ctx, TAIRSTRING_ERRORMSG_SYNTAX);
        return REDISMODULE_ERR;
    }

    TairStringObj *tair_string_obj = NULL;
    // 第0个是命令，有点类似 执行程序的时候，第0个是程序名。
    RedisModuleKey *key = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ | REDISMODULE_WRITE);
    // 函数指针，调用函数，获取key的类型。
    int type = RedisModule_KeyType(key);
    // `REDISMODULE_KEYTYPE_EMPTY` (0): 表示键在数据库中不存在。
    if (REDISMODULE_KEYTYPE_EMPTY == type) {
        // 如果类型是空的，说明key不存在。
        // 并且如果 xx 存在才设置，表示不满足条件，返回err 
        if (ex_flags & TAIR_STRING_SET_XX) {
            RedisModule_ReplyWithNull(ctx);
            return REDISMODULE_ERR;
        }
        // 没有xx的限制，就可以创建一个新的key。
        tair_string_obj = createTairStringTypeObject(); // 创建空白的 ts obj
        RedisModule_ModuleTypeSetValue(key, TairStringType, tair_string_obj);
    } else {
        // 如果key存在，并且不是ts类型，返回err
        if (RedisModule_ModuleTypeGetType(key) != TairStringType) {
            RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
            return REDISMODULE_ERR;
        }
        // 如果key存在，并且是ts类型，获取ts obj
        tair_string_obj = RedisModule_ModuleTypeGetValue(key);
        // 如果 nx 存在(也就是key不存在)才设置，表示不满足条件，返回err 【这个判断为什么不能前移？ 】
        if (ex_flags & TAIR_STRING_SET_NX) {
            RedisModule_ReplyWithNull(ctx);
            return REDISMODULE_ERR;
        }

        /* Version 0 means no version checking. */
        // 如果版本号不为0，并且版本号不匹配（更新操作的版本，与最新的不能对应上。 ），返回err
        if (ex_flags & TAIR_STRING_SET_WITH_VER && version != 0 && version != tair_string_obj->version) {
            RedisModule_ReplyWithError(ctx, TAIRSTRING_ERRORMSG_VERSION);
            return REDISMODULE_ERR;
        }
    }
    // 如果有绝对版本，则设置绝对版本，否则版本号+1
    if (ex_flags & TAIR_STRING_SET_WITH_ABS_VER) {
        tair_string_obj->version = version;
    } else {
        tair_string_obj->version++;
    }

    if (type != REDISMODULE_KEYTYPE_EMPTY) {
        /* Free the old value. */
        if (tair_string_obj->value) {
            RedisModule_FreeString(ctx, tair_string_obj->value);
        }
    }
    tair_string_obj->value = argv[2];
    /* Reuse the value to avoid memory copies. */
    RedisModule_RetainString(NULL, argv[2]);
    // flags 好像都没有使用。
    if (ex_flags & TAIR_STRING_SET_WITH_FLAGS) {
        tair_string_obj->flags = flags;
    }

    if (expire_p) {
        if (ex_flags & TAIR_STRING_SET_EX) {
            expire *= 1000;
        }
        if (ex_flags & TAIR_STRING_SET_ABS_EXPIRE) {
            /* Since the RedisModule_SetExpire interface can only set relative
            expiration times, here we first convert the absolute time passed
            in by the user to relative time. */
            milliseconds = expire - RedisModule_Milliseconds();
            if (milliseconds < 0) {
                /* Time out now. */
                milliseconds = 0;
            }
        } else {
            milliseconds = expire;
        }

        RedisModule_SetExpire(key, milliseconds);
    } else if (!(ex_flags & TAIR_STRING_SET_KEEPTTL)) {
        RedisModule_SetExpire(key, REDISMODULE_NO_EXPIRE);
    }

    /* Rewrite relative value to absolute value. */
    // 将相对值，转成绝对值。
    size_t vlen = 4, VSIZE_MAX = 8;
    RedisModuleString **v = NULL;
    v = RedisModule_Calloc(sizeof(RedisModuleString *), VSIZE_MAX);
    v[0] = RedisModule_CreateStringFromString(ctx, argv[1]);
    v[1] = RedisModule_CreateStringFromString(ctx, argv[2]);
    v[2] = RedisModule_CreateString(ctx, "ABS", 3);
    v[3] = RedisModule_CreateStringFromLongLong(ctx, tair_string_obj->version);
    if (expire_p) {
        v[vlen] = RedisModule_CreateString(ctx, "PXAT", 4);
        v[vlen + 1] = RedisModule_CreateStringFromLongLong(ctx, milliseconds + RedisModule_Milliseconds());
        vlen += 2;
    }
    if (flags_p) {
        v[vlen] = RedisModule_CreateString(ctx, "FLAGS", 5);
        v[vlen + 1] = RedisModule_CreateStringFromLongLong(ctx, (long long)tair_string_obj->flags);
        vlen += 2;
    }
    RedisModule_Replicate(ctx, "EXSET", "v", v, vlen);
    RedisModule_Free(v);

    if (ex_flags & TAIR_STRING_RETURN_WITH_VER) {
        RedisModule_ReplyWithLongLong(ctx, tair_string_obj->version);
    } else {
        RedisModule_ReplyWithSimpleString(ctx, "OK");
    }

    return REDISMODULE_OK;
}

/* EXGET <key> [WITHFLAGS] */
int TairStringTypeGet_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);
    // 只支持 2 个或者3个参数。
    if (argc < 2 || argc > 3) {
        return RedisModule_WrongArity(ctx);
    }
    // 文档中没有这个了。
    if (argc == 3 && mstringcasecmp(argv[2], "withflags")) {
        RedisModule_ReplyWithError(ctx, TAIRSTRING_ERRORMSG_SYNTAX);
        return REDISMODULE_ERR;
    }

    RedisModuleKey *key = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ);
    int type = RedisModule_KeyType(key);
    if (type != REDISMODULE_KEYTYPE_EMPTY && RedisModule_ModuleTypeGetType(key) != TairStringType) {
        return RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
    }

    if (type == REDISMODULE_KEYTYPE_EMPTY) {
        RedisModule_ReplyWithNull(ctx);
        return REDISMODULE_OK;
    }
    // get命令还是简单一些哦。 哈哈哈。
    TairStringObj *o = RedisModule_ModuleTypeGetValue(key);
    if (argc == 2) {
        // 熟悉的感觉，往cmd中添加响应的数据。
        RedisModule_ReplyWithArray(ctx, 2);
        RedisModule_ReplyWithString(ctx, o->value);
        RedisModule_ReplyWithLongLong(ctx, o->version);
    } else { /* argc == 3, WITHFLAGS .*/
        RedisModule_ReplyWithArray(ctx, 3);
        RedisModule_ReplyWithString(ctx, o->value);
        RedisModule_ReplyWithLongLong(ctx, o->version);
        RedisModule_ReplyWithLongLong(ctx, (long long)o->flags);
    }

    return REDISMODULE_OK;
}

/* EXINCRBY <key> <num> [DEF default_value] [EX/EXAT/PX/PXAT time] [NX/XX]
 * [VER/ABS version] [MIN/MAX maxval] [NONEGATIVE] [WITHVERSION] [KEEPTTL] */
int TairStringTypeIncrBy_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);

    if (argc < 3) {
        return RedisModule_WrongArity(ctx);
    }

    long long min = 0, max = 0, value, incr, defaultvalue = 0; /* If DEF is not set, then defaultvalue = 0 .*/
    RedisModuleString *min_p = NULL, *max_p = NULL;
    long long milliseconds = 0, expire = 0, version = 0;
    RedisModuleString *expire_p = NULL, *version_p = NULL, *defaultvalue_p = NULL;

    int ex_flags = TAIR_STRING_SET_NO_FLAGS;
    unsigned int allow_flags = TAIR_STRING_SET_NX | TAIR_STRING_SET_XX | TAIR_STRING_SET_EX | TAIR_STRING_SET_PX | 
                      TAIR_STRING_SET_ABS_EXPIRE | TAIR_STRING_SET_KEEPTTL | TAIR_STRING_SET_WITH_VER |
                      TAIR_STRING_SET_WITH_ABS_VER  | TAIR_STRING_RETURN_WITH_VER | TAIR_STRING_SET_WITH_DEF |
                      TAIR_STRING_SET_NONEGATIVE | TAIR_STRING_SET_WITH_BOUNDARY;
    // 这些指针获取的什么？ 为啥要用指针？ 
    if (parseAndGetExFlags(argv, argc, 3, &ex_flags, &expire_p, &version_p, NULL, &defaultvalue_p, &min_p, &max_p, allow_flags) != REDISMODULE_OK) {
        RedisModule_ReplyWithError(ctx, TAIRSTRING_ERRORMSG_SYNTAX);
        return REDISMODULE_ERR;
    }

    RedisModuleKey *key = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ | REDISMODULE_WRITE);
    int type = RedisModule_KeyType(key);
    if (REDISMODULE_KEYTYPE_EMPTY != type && RedisModule_ModuleTypeGetType(key) != TairStringType) {
        RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
        return REDISMODULE_ERR;
    }

    if (RedisModule_StringToLongLong(argv[2], &incr) != REDISMODULE_OK) {
        RedisModule_ReplyWithError(ctx, TAIRSTRING_ERRORMSG_NO_INT);
        return REDISMODULE_ERR;
    }

    if ((NULL != defaultvalue_p) && (RedisModule_StringToLongLong(defaultvalue_p, &defaultvalue) != REDISMODULE_OK)) {
        RedisModule_ReplyWithError(ctx, TAIRSTRING_ERRORMSG_NO_INT);
        return REDISMODULE_ERR;
    }

    if ((NULL != expire_p) && (RedisModule_StringToLongLong(expire_p, &expire) != REDISMODULE_OK)) {
        RedisModule_ReplyWithError(ctx, TAIRSTRING_ERRORMSG_SYNTAX);
        return REDISMODULE_ERR;
    }

    if ((NULL != version_p) && (RedisModule_StringToLongLong(version_p, &version) != REDISMODULE_OK)) {
        RedisModule_ReplyWithError(ctx, TAIRSTRING_ERRORMSG_SYNTAX);
        return REDISMODULE_ERR;
    }

    if ((expire_p && expire <=0) || version < 0) {
        RedisModule_ReplyWithError(ctx, TAIRSTRING_ERRORMSG_SYNTAX);
        return REDISMODULE_ERR;
    }

    if ((NULL != min_p) && (RedisModule_StringToLongLong(min_p, &min))) {
        RedisModule_ReplyWithError(ctx, TAIRSTRING_ERRORMSG_MIN_MAX);
        return REDISMODULE_ERR;
    }

    if ((NULL != max_p) && (RedisModule_StringToLongLong(max_p, &max))) {
        RedisModule_ReplyWithError(ctx, TAIRSTRING_ERRORMSG_MIN_MAX);
        return REDISMODULE_ERR;
    }

    if (NULL != min_p && NULL != max_p && max < min) {
        RedisModule_ReplyWithError(ctx, TAIRSTRING_ERRORMSG_MIN_MAX);
        return REDISMODULE_ERR;
    }

    TairStringObj *tair_string_obj = NULL;
    if (type == REDISMODULE_KEYTYPE_EMPTY) {
        if (ex_flags & TAIR_STRING_SET_XX) {
            RedisModule_ReplyWithNull(ctx);
            return REDISMODULE_ERR;
        }
        tair_string_obj = createTairStringTypeObject();
        value = defaultvalue;
    } else {
        if (ex_flags & TAIR_STRING_SET_NX) {
            RedisModule_ReplyWithNull(ctx);
            return REDISMODULE_ERR;
        }

        tair_string_obj = RedisModule_ModuleTypeGetValue(key);
        if (RedisModule_StringToLongLong(tair_string_obj->value, &value) != REDISMODULE_OK) {
            RedisModule_ReplyWithError(ctx, TAIRSTRING_ERRORMSG_NO_INT);
            return REDISMODULE_ERR;
        }

        if (ex_flags & TAIR_STRING_SET_WITH_VER && version != 0 && version != tair_string_obj->version) {
            RedisModule_ReplyWithError(ctx, TAIRSTRING_ERRORMSG_VERSION);
            return REDISMODULE_ERR;
        }
    }

    /* If DEF is set and the key is empty at first, the value won't increase, so
     * it's unnecessary to check overflow; else the value would increase by
     * incr, so overflow should be checked. "unnecessary to check overflow" =
     * "won't return ERR" = "no need to release object".
     * */
    if (!(ex_flags & TAIR_STRING_SET_WITH_DEF && type == REDISMODULE_KEYTYPE_EMPTY)) {
        /* Check overflow. */
        if ((incr < 0 && value < 0 && incr < (LLONG_MIN - value))
            || (incr > 0 && value > 0 && incr > (LLONG_MAX - value)) || (max_p != NULL && value + incr > max)
            || (min_p != NULL && value + incr < min)) {
            /* If type == EMPTY, then the tair_string_obj is created, so it
             * should be released here. */
            if (type == REDISMODULE_KEYTYPE_EMPTY && tair_string_obj) TairStringTypeReleaseObject(tair_string_obj);
            RedisModule_ReplyWithError(ctx, TAIRSTRING_ERRORMSG_OVERFLOW);
            return REDISMODULE_ERR;
        }
        value += incr;
    }

    /* value shouldn't be negative if NONEGATIVE is set; if value is negative,
     * let value = 0 */
    if (ex_flags & TAIR_STRING_SET_NONEGATIVE) value = value < 0 ? 0LL : value;

    if (type != REDISMODULE_KEYTYPE_EMPTY) {
        if (tair_string_obj->value) {
            RedisModule_FreeString(ctx, tair_string_obj->value);
            tair_string_obj->value = NULL;
        }
    } else {
        RedisModule_ModuleTypeSetValue(key, TairStringType, tair_string_obj);
    }

    tair_string_obj->value = RedisModule_CreateStringFromLongLong(NULL, value);

    if (ex_flags & TAIR_STRING_SET_WITH_ABS_VER) {
        tair_string_obj->version = version;
    } else {
        /* If the key doesn't exist and default is set, the version should be 1
         * although the value won't increase. */
        tair_string_obj->version++;
    }

    if (expire_p) {
        if (ex_flags & TAIR_STRING_SET_EX) {
            expire *= 1000;
        }
        if (ex_flags & TAIR_STRING_SET_ABS_EXPIRE) {
            milliseconds = expire - RedisModule_Milliseconds();
            if (milliseconds < 0) {
                milliseconds = 0;
            }
        } else {
            milliseconds = expire;
        }

        RedisModule_SetExpire(key, milliseconds);
    } else if (!(ex_flags & TAIR_STRING_SET_KEEPTTL)) {
        RedisModule_SetExpire(key, REDISMODULE_NO_EXPIRE);
    }

    if (expire_p) {
        RedisModule_Replicate(ctx, "EXSET", "ssclcl", argv[1], tair_string_obj->value, "ABS", tair_string_obj->version,
                              "PXAT", (milliseconds + RedisModule_Milliseconds()));
    } else {
        RedisModule_Replicate(ctx, "EXSET", "sscl", argv[1], tair_string_obj->value, "ABS", tair_string_obj->version);
    }

    if (ex_flags & TAIR_STRING_RETURN_WITH_VER) {
        RedisModule_ReplyWithArray(ctx, 2);
        RedisModule_ReplyWithLongLong(ctx, value);
        RedisModule_ReplyWithLongLong(ctx, tair_string_obj->version);
    } else {
        RedisModule_ReplyWithLongLong(ctx, value);
    }
    return REDISMODULE_OK;
}

/* EXINCRBYFLOAT <key> <num> [MIN/MAX maxval] [EX/EXAT/PX/PXAT time] [NX/XX] [VER/ABS version] [KEEPTTL] */
int TairStringTypeIncrByFloat_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);

    if (argc < 3) {
        return RedisModule_WrongArity(ctx);
    }

    long double min = 0, max = 0, value, oldvalue, incr;
    RedisModuleString *min_p = NULL, *max_p = NULL;
    long long milliseconds = 0, expire = 0, version = 0;
    RedisModuleString *expire_p = NULL, *version_p = NULL;

    RedisModuleKey *key = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ | REDISMODULE_WRITE);
    int type = RedisModule_KeyType(key);
    if (REDISMODULE_KEYTYPE_EMPTY != type && RedisModule_ModuleTypeGetType(key) != TairStringType) {
        RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
        return REDISMODULE_ERR;
    }

    if (mstring2ld(argv[2], &incr) == REDISMODULE_ERR) {
        RedisModule_ReplyWithError(ctx, TAIRSTRING_ERRORMSG_NO_FLOAT);
        return REDISMODULE_ERR;
    }

    int ex_flags = TAIR_STRING_SET_NO_FLAGS;
    unsigned int allow_flags = TAIR_STRING_SET_NX | TAIR_STRING_SET_XX | TAIR_STRING_SET_EX | TAIR_STRING_SET_PX | 
                      TAIR_STRING_SET_ABS_EXPIRE | TAIR_STRING_SET_KEEPTTL | TAIR_STRING_SET_WITH_VER |
                      TAIR_STRING_SET_WITH_ABS_VER | TAIR_STRING_SET_WITH_BOUNDARY;
    if (parseAndGetExFlags(argv, argc, 3, &ex_flags, &expire_p, &version_p, NULL, NULL, &min_p, &max_p, allow_flags) != REDISMODULE_OK) {
        RedisModule_ReplyWithError(ctx, TAIRSTRING_ERRORMSG_SYNTAX);
        return REDISMODULE_ERR;
    }

    if ((NULL != expire_p) && (RedisModule_StringToLongLong(expire_p, &expire) != REDISMODULE_OK)) {
        RedisModule_ReplyWithError(ctx, TAIRSTRING_ERRORMSG_SYNTAX);
        return REDISMODULE_ERR;
    }

    if ((NULL != version_p) && (RedisModule_StringToLongLong(version_p, &version) != REDISMODULE_OK)) {
        RedisModule_ReplyWithError(ctx, TAIRSTRING_ERRORMSG_SYNTAX);
        return REDISMODULE_ERR;
    }

    if ((expire_p && expire <=0) || version < 0) {
        RedisModule_ReplyWithError(ctx, TAIRSTRING_ERRORMSG_SYNTAX);
        return REDISMODULE_ERR;
    }

    if ((NULL != min_p) && (mstring2ld(min_p, &min) != REDISMODULE_OK)) {
        RedisModule_ReplyWithError(ctx, TAIRSTRING_ERRORMSG_MIN_MAX);
        return REDISMODULE_ERR;
    }

    if ((NULL != max_p) && (mstring2ld(max_p, &max) != REDISMODULE_OK)) {
        RedisModule_ReplyWithError(ctx, TAIRSTRING_ERRORMSG_MIN_MAX);
        return REDISMODULE_ERR;
    }

    if (NULL != min_p && NULL != max_p && max < min) {
        RedisModule_ReplyWithError(ctx, TAIRSTRING_ERRORMSG_MIN_MAX);
        return REDISMODULE_ERR;
    }

    TairStringObj *tair_string_obj = NULL;
    if (type == REDISMODULE_KEYTYPE_EMPTY) {
        if (ex_flags & TAIR_STRING_SET_XX) {
            RedisModule_ReplyWithNull(ctx);
            return REDISMODULE_ERR;
        }
        tair_string_obj = createTairStringTypeObject();
        value = 0;
    } else {
        if (ex_flags & TAIR_STRING_SET_NX) {
            RedisModule_ReplyWithNull(ctx);
            return REDISMODULE_ERR;
        }

        tair_string_obj = RedisModule_ModuleTypeGetValue(key);
        if (mstring2ld(tair_string_obj->value, &value) != REDISMODULE_OK) {
            RedisModule_ReplyWithError(ctx, TAIRSTRING_ERRORMSG_NO_FLOAT);
            return REDISMODULE_ERR;
        }

        if (ex_flags & TAIR_STRING_SET_WITH_VER && version != 0 && version != tair_string_obj->version) {
            RedisModule_ReplyWithError(ctx, TAIRSTRING_ERRORMSG_VERSION);
            return REDISMODULE_ERR;
        }
    }

    oldvalue = value;

    if (isnan(oldvalue + incr) || isinf(oldvalue + incr) || (max_p != NULL && oldvalue + incr > max)
        || (min_p != NULL && oldvalue + incr < min)) {
        if (type == REDISMODULE_KEYTYPE_EMPTY && tair_string_obj) TairStringTypeReleaseObject(tair_string_obj);
        RedisModule_ReplyWithError(ctx, TAIRSTRING_ERRORMSG_OVERFLOW);
        return REDISMODULE_ERR;
    }

    if (ex_flags & TAIR_STRING_SET_WITH_ABS_VER) {
        tair_string_obj->version = version;
    } else {
        tair_string_obj->version++;
    }

    value += incr;

    char dbuf[MAX_LONG_DOUBLE_CHARS];
    int dlen = m_ld2string(dbuf, sizeof(dbuf), value, 1);

    if (type != REDISMODULE_KEYTYPE_EMPTY) {
        if (tair_string_obj->value) {
            RedisModule_FreeString(ctx, tair_string_obj->value);
            tair_string_obj->value = NULL;
        }
    } else {
        RedisModule_ModuleTypeSetValue(key, TairStringType, tair_string_obj);
    }
    tair_string_obj->value = RedisModule_CreateString(NULL, dbuf, dlen);

    if (expire_p) {
        if (ex_flags & TAIR_STRING_SET_EX) {
            expire *= 1000;
        }
        if (ex_flags & TAIR_STRING_SET_ABS_EXPIRE) {
            milliseconds = expire - RedisModule_Milliseconds();
            if (milliseconds < 0) {
                milliseconds = 0;
            }
        } else {
            milliseconds = expire;
        }

        RedisModule_SetExpire(key, milliseconds);
    } else if (!(ex_flags & TAIR_STRING_SET_KEEPTTL)) {
        RedisModule_SetExpire(key, REDISMODULE_NO_EXPIRE);
    }

    if (expire_p) {
        RedisModule_Replicate(ctx, "EXSET", "ssclcl", argv[1], tair_string_obj->value, "ABS", tair_string_obj->version,
                              "PXAT", (milliseconds + RedisModule_Milliseconds()));
    } else {
        RedisModule_Replicate(ctx, "EXSET", "sscl", argv[1], tair_string_obj->value, "ABS", tair_string_obj->version);
    }

    RedisModule_ReplyWithString(ctx, tair_string_obj->value);
    return REDISMODULE_OK;
}

/* EXSETVER <key> <version> */
int TairStringTypeExSetVer_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);

    if (argc != 3) {
        return RedisModule_WrongArity(ctx);
    }

    long long version = 0;

    RedisModuleKey *key = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ | REDISMODULE_WRITE);
    int type = RedisModule_KeyType(key);
    if (REDISMODULE_KEYTYPE_EMPTY != type && RedisModule_ModuleTypeGetType(key) != TairStringType) {
        RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
        return REDISMODULE_ERR;
    }

    TairStringObj *tair_string_obj = NULL;
    if (type == REDISMODULE_KEYTYPE_EMPTY) {
        RedisModule_ReplyWithLongLong(ctx, 0);
        return REDISMODULE_OK;
    } else {
        tair_string_obj = RedisModule_ModuleTypeGetValue(key);
    }

    if (RedisModule_StringToLongLong(argv[2], &version) != REDISMODULE_OK) {
        RedisModule_ReplyWithError(ctx, TAIRSTRING_ERRORMSG_SYNTAX);
        return REDISMODULE_ERR;
    }

    if (version <= 0) {
        RedisModule_ReplyWithError(ctx, TAIRSTRING_ERRORMSG_SYNTAX);
        return REDISMODULE_ERR;
    }

    RedisModule_ReplicateVerbatim(ctx);
    tair_string_obj->version = version;
    RedisModule_ReplyWithLongLong(ctx, 1);
    return REDISMODULE_OK;
}

/* EXCAS <key> <new_value> <version> [EX/EXAT/PX/PXAT time] [KEEPTTL] */
int TairStringTypeExCas_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);

    if (argc < 4) {
        return RedisModule_WrongArity(ctx);
    }

    long long version = 0;
    long long milliseconds = 0, expire = 0;
    RedisModuleString *expire_p = NULL;
    int ex_flags = TAIR_STRING_SET_NO_FLAGS;
    unsigned int allow_flags = TAIR_STRING_SET_EX | TAIR_STRING_SET_PX | TAIR_STRING_SET_ABS_EXPIRE | TAIR_STRING_SET_KEEPTTL;
    if (parseAndGetExFlags(argv, argc, 4, &ex_flags, &expire_p, NULL, NULL, NULL, NULL, NULL, allow_flags) != REDISMODULE_OK) {
        RedisModule_ReplyWithError(ctx, TAIRSTRING_ERRORMSG_SYNTAX);
        return REDISMODULE_ERR;
    }

    if ((NULL != expire_p) && (RedisModule_StringToLongLong(expire_p, &expire) != REDISMODULE_OK)) {
        RedisModule_ReplyWithError(ctx, TAIRSTRING_ERRORMSG_SYNTAX);
        return REDISMODULE_ERR;
    }

    if (RedisModule_StringToLongLong(argv[3], &version) != REDISMODULE_OK) {
        RedisModule_ReplyWithError(ctx, TAIRSTRING_ERRORMSG_VER_INT);
        return REDISMODULE_ERR;
    }

    if ((expire_p && expire <=0) || version < 0) {
        RedisModule_ReplyWithError(ctx, TAIRSTRING_ERRORMSG_SYNTAX);
        return REDISMODULE_ERR;
    }

    RedisModuleKey *key = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_WRITE);
    int type = RedisModule_KeyType(key);
    if (REDISMODULE_KEYTYPE_EMPTY != type && RedisModule_ModuleTypeGetType(key) != TairStringType) {
        RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
        return REDISMODULE_ERR;
    }

    TairStringObj *tair_string_obj = NULL;
    if (type == REDISMODULE_KEYTYPE_EMPTY) {
        RedisModule_ReplyWithLongLong(ctx, -1);
        return REDISMODULE_OK;
    } else {
        tair_string_obj = RedisModule_ModuleTypeGetValue(key);
    }

    if (tair_string_obj->version != version) {
        RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
        /* Here we can not use RedisModule_ReplyWithError directly, because this
        will cause jedis throw an exception, and the client can not read the
        later version and value. */
        RedisModule_ReplyWithSimpleString(ctx, TAIRSTRING_STATUSMSG_VERSION);
        RedisModule_ReplyWithString(ctx, tair_string_obj->value);
        RedisModule_ReplyWithLongLong(ctx, tair_string_obj->version);
        RedisModule_ReplySetArrayLength(ctx, 3);
        return REDISMODULE_ERR;
    }

    if (tair_string_obj->value) {
        RedisModule_FreeString(ctx, tair_string_obj->value);
        tair_string_obj->value = NULL;
    }
    tair_string_obj->value = argv[2];
    RedisModule_RetainString(NULL, argv[2]);
    tair_string_obj->version++;

    if (expire_p) {
        if (ex_flags & TAIR_STRING_SET_EX) {
            expire *= 1000;
        }
        if (ex_flags & TAIR_STRING_SET_ABS_EXPIRE) {
            milliseconds = expire - RedisModule_Milliseconds();
            if (milliseconds < 0) {
                milliseconds = 0;
            }
        } else {
            milliseconds = expire;
        }

        RedisModule_SetExpire(key, milliseconds);
    } else if (!(ex_flags & TAIR_STRING_SET_KEEPTTL)) {
        RedisModule_SetExpire(key, REDISMODULE_NO_EXPIRE);
    }

    if (expire_p) {
        RedisModule_Replicate(ctx, "EXSET", "ssclcl", argv[1], tair_string_obj->value, "ABS", tair_string_obj->version,
                              "PXAT", (milliseconds + RedisModule_Milliseconds()));
    } else {
        RedisModule_Replicate(ctx, "EXSET", "sscl", argv[1], tair_string_obj->value, "ABS", tair_string_obj->version);
    }

    RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
    RedisModule_ReplyWithSimpleString(ctx, "OK");
    RedisModule_ReplyWithSimpleString(ctx, "");
    RedisModule_ReplyWithLongLong(ctx, tair_string_obj->version);
    RedisModule_ReplySetArrayLength(ctx, 3);
    return REDISMODULE_OK;
}

/* EXCAD <key> <version> */
int TairStringTypeExCad_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);

    if (argc != 3) {
        return RedisModule_WrongArity(ctx);
    }

    long long version = 0;

    RedisModuleKey *key = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ | REDISMODULE_WRITE);
    int type = RedisModule_KeyType(key);
    if (REDISMODULE_KEYTYPE_EMPTY != type && RedisModule_ModuleTypeGetType(key) != TairStringType) {
        RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
        return REDISMODULE_ERR;
    }

    if (RedisModule_StringToLongLong(argv[2], &version) != REDISMODULE_OK) {
        RedisModule_ReplyWithError(ctx, TAIRSTRING_ERRORMSG_SYNTAX);
        return REDISMODULE_ERR;
    }

    TairStringObj *tair_string_obj = NULL;
    if (type == REDISMODULE_KEYTYPE_EMPTY) {
        RedisModule_ReplyWithLongLong(ctx, -1);
        return REDISMODULE_OK;
    } else {
        tair_string_obj = RedisModule_ModuleTypeGetValue(key);
    }

    if (tair_string_obj->version != version) {
        RedisModule_ReplyWithLongLong(ctx, 0);
        return REDISMODULE_OK;
    }

    RedisModule_Replicate(ctx, "DEL", "s", argv[1]);
    RedisModule_DeleteKey(key);
    RedisModule_ReplyWithLongLong(ctx, 1);
    return REDISMODULE_OK;
}

/* CAD <key> <value> */
int StringTypeCad_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);

    if (argc != 3) {
        return RedisModule_WrongArity(ctx);
    }

    RedisModuleKey *key = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_WRITE);
    int type = RedisModule_KeyType(key);
    if (REDISMODULE_KEYTYPE_EMPTY != type && type != REDISMODULE_KEYTYPE_STRING) {
        RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
        return REDISMODULE_ERR;
    }

    if (type == REDISMODULE_KEYTYPE_EMPTY) {
        RedisModule_ReplyWithLongLong(ctx, -1);
        return REDISMODULE_OK;
    } else {
        size_t proto_len, expect_len;
        RedisModuleCallReply *replay = RedisModule_Call(ctx, "GET", "s", argv[1]);
        switch (RedisModule_CallReplyType(replay)) {
            case REDISMODULE_REPLY_STRING:
                break;
            default:
                RedisModule_ReplyWithLongLong(ctx, 0);
                return REDISMODULE_OK;
        }

        const char *proto_ptr = RedisModule_CallReplyStringPtr(replay, &proto_len);
        const char *expect_ptr = RedisModule_StringPtrLen(argv[2], &expect_len);
        if (proto_len != expect_len || memcmp(expect_ptr, proto_ptr, proto_len) != 0) {
            RedisModule_ReplyWithLongLong(ctx, 0);
            return REDISMODULE_OK;
        }
    }

    RedisModule_DeleteKey(key);
    RedisModule_Replicate(ctx, "DEL", "s", argv[1]);
    RedisModule_ReplyWithLongLong(ctx, 1);
    return REDISMODULE_OK;
}

/* CAS <Key> <oldvalue> <newvalue> [EX/EXAT/PX/PXAT time] [KEEPTTL] */
int StringTypeCas_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);

    if (argc < 4) {
        return RedisModule_WrongArity(ctx);
    }

    long long milliseconds = 0, expire = 0;
    RedisModuleString *expire_p = NULL;
    int ex_flags = TAIR_STRING_SET_NO_FLAGS;
    unsigned int allow_flags = TAIR_STRING_SET_EX | TAIR_STRING_SET_PX | TAIR_STRING_SET_ABS_EXPIRE | TAIR_STRING_SET_KEEPTTL;
    if (parseAndGetExFlags(argv, argc, 4, &ex_flags, &expire_p, NULL, NULL, NULL, NULL, NULL, allow_flags) != REDISMODULE_OK) {
        RedisModule_ReplyWithError(ctx, TAIRSTRING_ERRORMSG_SYNTAX);
        return REDISMODULE_ERR;
    }

    if ((NULL != expire_p) && (RedisModule_StringToLongLong(expire_p, &expire) != REDISMODULE_OK)) {
        RedisModule_ReplyWithError(ctx, TAIRSTRING_ERRORMSG_SYNTAX);
        return REDISMODULE_ERR;
    }

    if ((expire_p && expire <=0)) {
        RedisModule_ReplyWithError(ctx, TAIRSTRING_ERRORMSG_SYNTAX);
        return REDISMODULE_ERR;
    }

    RedisModuleKey *key = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_WRITE);
    int type = RedisModule_KeyType(key);
    if (REDISMODULE_KEYTYPE_EMPTY != type && type != REDISMODULE_KEYTYPE_STRING) {
        RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
        return REDISMODULE_ERR;
    }

    if (type == REDISMODULE_KEYTYPE_EMPTY) {
        RedisModule_ReplyWithLongLong(ctx, -1);
        return REDISMODULE_OK;
    } else {
        size_t proto_len, expect_len;
        RedisModuleCallReply *replay = RedisModule_Call(ctx, "GET", "s", argv[1]);
        switch (RedisModule_CallReplyType(replay)) {
            case REDISMODULE_REPLY_STRING:
                break;
            default:
                RedisModule_ReplyWithLongLong(ctx, 0);
                return REDISMODULE_OK;
        }

        const char *proto_ptr = RedisModule_CallReplyStringPtr(replay, &proto_len);
        const char *expect_ptr = RedisModule_StringPtrLen(argv[2], &expect_len);
        if (proto_len != expect_len || memcmp(expect_ptr, proto_ptr, proto_len) != 0) {
            RedisModule_ReplyWithLongLong(ctx, 0);
            return REDISMODULE_OK;
        }
    }

    if (RedisModule_StringSet(key, argv[3]) != REDISMODULE_OK) {
        return REDISMODULE_ERR;
    }

    if (expire_p) {
        if (ex_flags & TAIR_STRING_SET_EX) {
            expire *= 1000;
        }
        if (ex_flags & TAIR_STRING_SET_ABS_EXPIRE) {
            milliseconds = expire - RedisModule_Milliseconds();
            if (milliseconds < 0) {
                milliseconds = 0;
            }
        } else {
            milliseconds = expire;
        }

        RedisModule_SetExpire(key, milliseconds);
    } else if (!(ex_flags & TAIR_STRING_SET_KEEPTTL)) {
        RedisModule_SetExpire(key, REDISMODULE_NO_EXPIRE);
    }

    RedisModule_Replicate(ctx, "SET", "ss", argv[1], argv[3]);
    if (expire_p) {
        RedisModule_Replicate(ctx, "PEXPIREAT", "sl", argv[1], (milliseconds + RedisModule_Milliseconds()));
    }

    RedisModule_ReplyWithLongLong(ctx, 1);
    return REDISMODULE_OK;
}
// 文档中，没有写，不看。
/* EXPREPEND <key> <value> [NX|XX] [VER/ABS version] */
int TairStringTypeExPrepend_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);

    if (argc < 3) {
        return RedisModule_WrongArity(ctx);
    }

    RedisModuleString *version_p = NULL;
    long long version = 0;
    int ex_flags = TAIR_STRING_SET_NO_FLAGS;
    unsigned int allow_flags = TAIR_STRING_SET_NX | TAIR_STRING_SET_XX | TAIR_STRING_SET_WITH_VER | TAIR_STRING_SET_WITH_ABS_VER;
    if (parseAndGetExFlags(argv, argc, 3, &ex_flags, NULL, &version_p, NULL, NULL, NULL, NULL, allow_flags) != REDISMODULE_OK) {
        RedisModule_ReplyWithError(ctx, TAIRSTRING_ERRORMSG_SYNTAX);
        return REDISMODULE_ERR;
    }

    if ((NULL != version_p) && (RedisModule_StringToLongLong(version_p, &version) != REDISMODULE_OK)) {
        RedisModule_ReplyWithError(ctx, TAIRSTRING_ERRORMSG_SYNTAX);
        return REDISMODULE_ERR;
    }

    if (version < 0) {
        RedisModule_ReplyWithError(ctx, TAIRSTRING_ERRORMSG_SYNTAX);
        return REDISMODULE_ERR;
    }

    size_t originalLength;
    RedisModuleKey *key = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_WRITE);
    RedisModuleString *newvalue;
    int type = RedisModule_KeyType(key);

    TairStringObj *tair_string_obj = NULL;

    if (type == REDISMODULE_KEYTYPE_EMPTY) {
        /* not exist: result = argv[2] */
        if (ex_flags & TAIR_STRING_SET_XX) {
            RedisModule_ReplyWithNull(ctx);
            return REDISMODULE_ERR;
        }
        tair_string_obj = createTairStringTypeObject();
        tair_string_obj->value = argv[2];
        RedisModule_ModuleTypeSetValue(key, TairStringType, tair_string_obj);
        RedisModule_RetainString(ctx, argv[2]);
    } else {
        /* exist: result = argv[2] + original */
        /* Statements like "tair_string_obj->value = argv[2];
        Append(tair_string_obj->value, original);" would lead to a mistake when
        replicating the command from master to slave, so it's necessary to
        create a copy of argv[2] and use the copy to get the command executed.
      */

        if (ex_flags & TAIR_STRING_SET_NX) {
            RedisModule_ReplyWithNull(ctx);
            return REDISMODULE_ERR;
        }

        if (RedisModule_ModuleTypeGetType(key) != TairStringType) {
            RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
            return REDISMODULE_ERR;
        }
        tair_string_obj = RedisModule_ModuleTypeGetValue(key);

        if (ex_flags & TAIR_STRING_SET_WITH_VER && version != 0 && version != tair_string_obj->version) {
            RedisModule_ReplyWithError(ctx, TAIRSTRING_ERRORMSG_VERSION);
            return REDISMODULE_ERR;
        }

        /* Convert RedisModuleString to cstring to use StringAppendBuffer() */
        const char *c_string_original = RedisModule_StringPtrLen(tair_string_obj->value, &originalLength);
        newvalue = RedisModule_CreateStringFromString(ctx, argv[2]);

        if (RedisModule_StringAppendBuffer(ctx, newvalue, c_string_original, originalLength) == REDISMODULE_ERR) {
            RedisModule_ReplyWithError(ctx, TAIRSTRING_ERRORMSG_APPENDBUFFER);
            return REDISMODULE_ERR;
        }

        if (tair_string_obj->value) {
            RedisModule_FreeString(ctx, tair_string_obj->value);
        }

        tair_string_obj->value = newvalue;
        RedisModule_RetainString(NULL, newvalue);
    }

    if (ex_flags & TAIR_STRING_SET_WITH_ABS_VER) {
        tair_string_obj->version = version;
    } else {
        tair_string_obj->version++;
    }

    RedisModule_ReplicateVerbatim(ctx);
    RedisModule_ReplyWithLongLong(ctx, tair_string_obj->version);
    return REDISMODULE_OK;
}

// 文档中，没有写，不看。
/* EXAPPEND <key> <value> [NX|XX] [VER/ABS version] */
int TairStringTypeExAppend_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);
    if (argc < 3) {
        return RedisModule_WrongArity(ctx);
    }

    RedisModuleString *version_p = NULL;
    long long version = 0;
    int ex_flags = TAIR_STRING_SET_NO_FLAGS;
    unsigned int allow_flags = TAIR_STRING_SET_NX | TAIR_STRING_SET_XX | TAIR_STRING_SET_WITH_VER | TAIR_STRING_SET_WITH_ABS_VER;
    if (parseAndGetExFlags(argv, argc, 3, &ex_flags, NULL, &version_p, NULL, NULL, NULL, NULL, allow_flags) != REDISMODULE_OK) {
        RedisModule_ReplyWithError(ctx, TAIRSTRING_ERRORMSG_SYNTAX);
        return REDISMODULE_ERR;
    }

    if ((NULL != version_p) && (RedisModule_StringToLongLong(version_p, &version) != REDISMODULE_OK)) {
        RedisModule_ReplyWithError(ctx, TAIRSTRING_ERRORMSG_SYNTAX);
        return REDISMODULE_ERR;
    }

    if (version < 0) {
        RedisModule_ReplyWithError(ctx, TAIRSTRING_ERRORMSG_SYNTAX);
        return REDISMODULE_ERR;
    }

    size_t appendLength;
    RedisModuleKey *key = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_WRITE);
    int type = RedisModule_KeyType(key);

    TairStringObj *tair_string_obj = NULL;
    if (type == REDISMODULE_KEYTYPE_EMPTY) {
        /* not exist: result = argv[2] */
        if (ex_flags & TAIR_STRING_SET_XX) {
            RedisModule_ReplyWithNull(ctx);
            return REDISMODULE_ERR;
        }

        tair_string_obj = createTairStringTypeObject();
        tair_string_obj->value = argv[2];
        RedisModule_ModuleTypeSetValue(key, TairStringType, tair_string_obj);
        RedisModule_RetainString(NULL, argv[2]);

    } else {
        /* exist: result = original + argv[2] */
        if (ex_flags & TAIR_STRING_SET_NX) {
            RedisModule_ReplyWithNull(ctx);
            return REDISMODULE_ERR;
        }
        if (RedisModule_ModuleTypeGetType(key) != TairStringType) {
            RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
            return REDISMODULE_ERR;
        }
        tair_string_obj = RedisModule_ModuleTypeGetValue(key);

        if (ex_flags & TAIR_STRING_SET_WITH_VER && version != 0 && version != tair_string_obj->version) {
            RedisModule_ReplyWithError(ctx, TAIRSTRING_ERRORMSG_VERSION);
            return REDISMODULE_ERR;
        }

        /* Convert RedisModuleString to cstring to use StringAppendBuffer() */
        const char *c_string_argv = RedisModule_StringPtrLen(argv[2], &appendLength);

        if (RedisModule_StringAppendBuffer(ctx, tair_string_obj->value, c_string_argv, appendLength)
            == REDISMODULE_ERR) {
            RedisModule_ReplyWithError(ctx, TAIRSTRING_ERRORMSG_APPENDBUFFER);
            return REDISMODULE_ERR;
        }
    }

    if (ex_flags & TAIR_STRING_SET_WITH_ABS_VER) {
        tair_string_obj->version = version;
    } else {
        tair_string_obj->version++;
    }

    RedisModule_ReplicateVerbatim(ctx);
    RedisModule_ReplyWithLongLong(ctx, tair_string_obj->version);
    return REDISMODULE_OK;
}

/* EXGAE <key> <EX time | EXAT time | PX time | PXAT time> */
int TairStringTypeExGAE_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);

    if (argc < 4) {
        return RedisModule_WrongArity(ctx);
    }
 
    RedisModuleString *expire_p;
    long long expire = 0, milliseconds = 0;
    int ex_flags = TAIR_STRING_SET_NO_FLAGS;
    unsigned int allow_flags = TAIR_STRING_SET_EX | TAIR_STRING_SET_PX | TAIR_STRING_SET_ABS_EXPIRE;
    if (parseAndGetExFlags(argv, argc, 2, &ex_flags, &expire_p, NULL, NULL, NULL, NULL, NULL, allow_flags) != REDISMODULE_OK) {
        RedisModule_ReplyWithError(ctx, TAIRSTRING_ERRORMSG_SYNTAX);
        return REDISMODULE_ERR;
    }

    if (RedisModule_StringToLongLong(expire_p, &expire) != REDISMODULE_OK) {
        RedisModule_ReplyWithError(ctx, TAIRSTRING_ERRORMSG_SYNTAX);
        return REDISMODULE_ERR;
    }

    if ((expire_p && expire <=0)) {
        RedisModule_ReplyWithError(ctx, TAIRSTRING_ERRORMSG_SYNTAX);
        return REDISMODULE_ERR;
    }

    RedisModuleKey *key = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_WRITE);
    int type = RedisModule_KeyType(key);
    if (type != REDISMODULE_KEYTYPE_EMPTY && RedisModule_ModuleTypeGetType(key) != TairStringType) {
        return RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
    }

    if (type == REDISMODULE_KEYTYPE_EMPTY) {
        RedisModule_ReplyWithNull(ctx);
        return REDISMODULE_OK;
    }

    if (expire_p) {
        if (ex_flags & TAIR_STRING_SET_EX) {
            expire *= 1000;
        }
        if (ex_flags & TAIR_STRING_SET_ABS_EXPIRE) {
            milliseconds = expire - RedisModule_Milliseconds();
            if (milliseconds < 0) {
                milliseconds = 0;
            }
        } else {
            milliseconds = expire;
        }

        RedisModule_SetExpire(key, milliseconds);
    }

    TairStringObj *o = RedisModule_ModuleTypeGetValue(key);

    RedisModule_ReplicateVerbatim(ctx);

    RedisModule_ReplyWithArray(ctx, 3);
    RedisModule_ReplyWithString(ctx, o->value);
    RedisModule_ReplyWithLongLong(ctx, o->version);
    RedisModule_ReplyWithLongLong(ctx, (long long)o->flags);
    return REDISMODULE_OK;
}

/* ========================== "exstrtype" type methods =======================*/
// 估计需要定义一些方法，供redis module 调用。
void *TairStringTypeRdbLoad(RedisModuleIO *rdb, int encver) {
    if (encver != TAIRSTRING_ENCVER_VER_1) {
        return NULL;
    }
    TairStringObj *o = createTairStringTypeObject();
    // 熟悉的方法。【从rdb中读取不同的字段。】
    o->version = RedisModule_LoadUnsigned(rdb);
    o->flags = RedisModule_LoadUnsigned(rdb);
    o->value = RedisModule_LoadString(rdb);
    return o;
}

void TairStringTypeRdbSave(RedisModuleIO *rdb, void *value) {
    const struct TairStringObj *o = value;
    assert(value != NULL);
    // 熟悉的方法。 
    RedisModule_SaveUnsigned(rdb, o->version);
    RedisModule_SaveUnsigned(rdb, o->flags);
    RedisModule_SaveString(rdb, o->value);
}

void TairStringTypeAofRewrite(RedisModuleIO *aof, RedisModuleString *key, void *value) {
    const struct TairStringObj *o = value;
    assert(value != NULL);
    // emit 是写入aof文件中。
    RedisModule_EmitAOF(aof, "EXSET", "ssclcl", key, o->value, "ABS", o->version, "FLAGS", (long long)o->flags);
}

size_t TairStringTypeMemUsage(const void *value) {
    const struct TairStringObj *o = value;
    assert(value != NULL);
    size_t len;
    RedisModule_StringPtrLen(o->value, &len);
    // key 和 value 的大小。  版本号啥的，在key里面。 value也是一个string。
    // 使用RedisModule_StringPtrLen 就可以获取到。
    return sizeof(*o) + len;
}

void TairStringTypeFree(void *value) { TairStringTypeReleaseObject(value); }

void TairStringTypeDigest(RedisModuleDigest *md, void *value) {
    const struct TairStringObj *o = value;
    assert(value != NULL);
    RedisModule_DigestAddLongLong(md, o->version);
    RedisModule_DigestAddLongLong(md, o->flags);
    size_t len;
    const char *str = RedisModule_StringPtrLen(o->value, &len);
    RedisModule_DigestAddStringBuffer(md, (unsigned char *)str, len);
    RedisModule_DigestEndSequence(md);
}
/*


*/
int Module_CreateCommands(RedisModuleCtx *ctx) {
#define CREATE_CMD(name, tgt, attr)                                                       \
    do {                                                                                  \
        if (RedisModule_CreateCommand(ctx, name, tgt, attr, 1, 1, 1) != REDISMODULE_OK) { \
            return REDISMODULE_ERR;                                                       \
        }                                                                                 \
    } while (0);
/*
CREATE_CMD 是一个宏，用于简化创建命令的代码。
name 是命令的名称。
tgt 是命令的实现函数。
attr 是命令的属性，例如 "write deny-oom" 表示这是一个写命令，不允许在内存不足时OOM（Out of Memory）。
宏内部使用
*/
#define CREATE_WRCMD(name, tgt) CREATE_CMD(name, tgt, "write deny-oom")
#define CREATE_ROCMD(name, tgt) CREATE_CMD(name, tgt, "readonly fast")
    // 区分读写命令。
    CREATE_WRCMD("exset", TairStringTypeSet_RedisCommand)
    CREATE_ROCMD("exget", TairStringTypeGet_RedisCommand)
    CREATE_WRCMD("exincrby", TairStringTypeIncrBy_RedisCommand)
    CREATE_WRCMD("exincrbyfloat", TairStringTypeIncrByFloat_RedisCommand)
    CREATE_WRCMD("exsetver", TairStringTypeExSetVer_RedisCommand)
    CREATE_WRCMD("excas", TairStringTypeExCas_RedisCommand)
    CREATE_WRCMD("excad", TairStringTypeExCad_RedisCommand)
    CREATE_WRCMD("exprepend", TairStringTypeExPrepend_RedisCommand)
    CREATE_WRCMD("exappend", TairStringTypeExAppend_RedisCommand)
    CREATE_WRCMD("exgae", TairStringTypeExGAE_RedisCommand)
    /* CAS/CAD cmds for redis string type. */
    CREATE_WRCMD("cas", StringTypeCas_RedisCommand)
    CREATE_WRCMD("cad", StringTypeCad_RedisCommand)

    return REDISMODULE_OK;
}
/*
这段代码是一个Redis模块的加载函数，用于在Redis服务器启动时初始化自定义数据类型和命令。


*/
int RedisModule_OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    // REDISMODULE_NOT_USED 宏用于告诉编译器这些参数没有被使用，避免编译器发出警告。
    REDISMODULE_NOT_USED(argv);
    REDISMODULE_NOT_USED(argc);

    if (RedisModule_Init(ctx, "exstrtype", 1, REDISMODULE_APIVER_1) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }
    /*
    RedisModuleTypeMethods 结构体定义了自定义数据类型的方法。
    version 是方法版本号。
    rdb_load、rdb_save、aof_rewrite、mem_usage、free 和 digest 
    是与Redis的RDB持久化和AOF重写相关的回调函数，
    用于定义如何序列化和反序列化数据，计算内存使用量，
    以及生成数据摘要。
    */
    RedisModuleTypeMethods tm = {.version = REDISMODULE_TYPE_METHOD_VERSION,
                                 .rdb_load = TairStringTypeRdbLoad,
                                 .rdb_save = TairStringTypeRdbSave,
                                 .aof_rewrite = TairStringTypeAofRewrite,
                                 .mem_usage = TairStringTypeMemUsage,
                                 .free = TairStringTypeFree,
                                 .digest = TairStringTypeDigest};
    /*
    RedisModule_CreateDataType 函数用于创建自定义数据类型。
    ctx 是模块上下文。
    "exstrtype" 是数据类型的名称。
    TAIRSTRING_ENCVER_VER_1 是数据类型的编码版本号。
    &tm 是数据类型的方法。
    如果创建失败，返回 REDISMODULE_ERR。
    */
    TairStringType = RedisModule_CreateDataType(ctx, "exstrtype", TAIRSTRING_ENCVER_VER_1, &tm);
    if (TairStringType == NULL) {
        return REDISMODULE_ERR;
    }
    /*
    Module_CreateCommands 函数用于创建模块的自定义命令。
    如果创建命令失败，返回 REDISMODULE_ERR。
    */
    if (REDISMODULE_ERR == Module_CreateCommands(ctx)) {
        return REDISMODULE_ERR;
    }

    return REDISMODULE_OK;
}
