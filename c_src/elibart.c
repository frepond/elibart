// -------------------------------------------------------------------
//
// elibart: Erlang Wrapper for Adaptive Radix Trees Lib (https://github.com/armon/libart)
//
// Copyright (C) Ngineo Limited 2011 - 2103. All rights reserved.
//
// This file is provided to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file
// except in compliance with the License.  You may obtain
// a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
//
// Notes:
//   These examples provided me usefull insights for building this NIF
//
//      https://github.com/davisp/nif-examples/
// -------------------------------------------------------------------

#include "erl_nif.h"
#include "art.h"
#include <string.h>
#include <stdio.h>

static ErlNifResourceType* elibart_RESOURCE = NULL;

typedef struct
{
    size_t size;
    unsigned char *data;
} art_elem_struct;

typedef struct $
{
    ErlNifEnv *env, *msg_env;
    ErlNifPid pid;
    ERL_NIF_TERM caller_ref;   
} callback_data;

// Prototypes
static ERL_NIF_TERM elibart_new(ErlNifEnv* env, int argc,
                                   const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM elibart_insert(ErlNifEnv* env, int argc,
                                          const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM elibart_search(ErlNifEnv* env, int argc,
                                          const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM elibart_prefix_search(ErlNifEnv* env, int argc,
                                          const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM elibart_size(ErlNifEnv* env, int argc,
                                          const ERL_NIF_TERM argv[]);

/* Some utility fucntions */
void printBinary(ErlNifBinary* bin)
{
    printf("%zu|", bin->size);
    fwrite(bin->data, 1, bin->size, stdout);
    fputs("|", stdout);
}

ERL_NIF_TERM
mk_atom(ErlNifEnv* env, const char* atom)
{
    ERL_NIF_TERM ret;

    if(!enif_make_existing_atom(env, atom, &ret, ERL_NIF_LATIN1))
        return enif_make_atom(env, atom);

    return ret;
}

ERL_NIF_TERM
mk_error(ErlNifEnv* env, const char* mesg)
{
    return enif_make_tuple(env, mk_atom(env, "error"), mk_atom(env, mesg));
}


/* implemented nifs */

static ErlNifFunc nif_funcs[] =
{
    {"new", 0, elibart_new},
    {"insert", 3, elibart_insert},
    {"search", 2, elibart_search},
    {"async_prefix_search", 4, elibart_prefix_search},
    {"art_size", 1, elibart_size}
};

static ERL_NIF_TERM elibart_new(ErlNifEnv* env, int argc,
                                   const ERL_NIF_TERM argv[])
{
    art_tree* t = enif_alloc_resource(elibart_RESOURCE,
                                                    sizeof(art_tree));
    int res = init_art_tree(t);
    
    if (res != 0)
        return enif_make_tuple2(env, enif_make_atom(env, "fail"), res);
    else 
    {
        ERL_NIF_TERM result = enif_make_resource(env, t);
        enif_release_resource(t);
    
        return enif_make_tuple2(env, enif_make_atom(env, "ok"), result);
    }
}


static ERL_NIF_TERM elibart_insert(ErlNifEnv* env, int argc,
                                          const ERL_NIF_TERM argv[])
{
    art_tree* t;
    ErlNifBinary key, value;
    char* key_copy;
    art_elem_struct *elem;

    // extract arguments atr_tree, key, value
    if(!enif_get_resource(env, argv[0], elibart_RESOURCE, (void**) &t))
        return enif_make_badarg(env);
    if (!enif_inspect_binary(env, argv[1], &key))
        return enif_make_badarg(env);
    if (!enif_inspect_binary(env, argv[2], &value))
        return enif_make_badarg(env);

    key_copy = malloc(key.size * sizeof(unsigned char));
    elem = malloc(sizeof(art_elem_struct));
    elem->data = malloc(value.size * sizeof(unsigned char));

    elem->size = value.size;
    memcpy(key_copy, key.data, key.size * sizeof(unsigned char));
    memcpy(elem->data, value.data, value.size * sizeof(unsigned char));

    art_elem_struct *old_elem = art_insert(t, (char*) key_copy, key.size, elem);
    
    if (!old_elem)
        return enif_make_tuple2(env, enif_make_atom(env, "ok"), enif_make_atom(env, "empty"));

    ErlNifBinary res;

    enif_alloc_binary(old_elem->size * sizeof(unsigned char), &res);
    memcpy(res.data, old_elem->data, old_elem->size * sizeof(unsigned char));

    free(key_copy);
    free(old_elem->data);
    free(old_elem);

    return enif_make_tuple2(env, enif_make_atom(env, "ok"), enif_make_binary(env, &res));
}

static ERL_NIF_TERM elibart_search(ErlNifEnv* env, int argc,
                                          const ERL_NIF_TERM argv[])
{
    art_tree* t;
    ErlNifBinary key;
    
    // extract arguments atr_tree, key
    if(!enif_get_resource(env, argv[0], elibart_RESOURCE, (void**) &t))
        return enif_make_badarg(env);
    if (!enif_inspect_binary(env, argv[1], &key))
        return enif_make_badarg(env);

    art_elem_struct *value = art_search(t, (char*) key.data, key.size);

    if (!value)
        return enif_make_atom(env, "empty");

    ErlNifBinary res;
    enif_alloc_binary(value->size * sizeof(unsigned char), &res);
    memcpy(res.data, value->data, value->size * sizeof(unsigned char));

    return enif_make_tuple2(env, enif_make_atom(env, "ok"), enif_make_binary(env, &res));
}

static int prefix_cb(void *data, const char *k, uint32_t k_len, void *val) {
    callback_data *cb_data = data;
    art_elem_struct *elem = val;
    ErlNifBinary key, value;

    enif_alloc_binary(k_len * sizeof(unsigned char), &key);
    memcpy(key.data, k, k_len);

    enif_alloc_binary(elem->size * sizeof(unsigned char), &value);
    memcpy(value.data, elem->data, elem->size * sizeof(unsigned char));

    ERL_NIF_TERM res = enif_make_tuple2(cb_data->msg_env, 
        cb_data->caller_ref,
        enif_make_tuple2(cb_data->msg_env, 
            enif_make_binary(cb_data->msg_env, &key), enif_make_binary(cb_data->msg_env, &value)));
    
    if(!enif_send(cb_data->env, &cb_data->pid, cb_data->msg_env, res)) 
        return -1;    

    return 0;
}


static ERL_NIF_TERM elibart_prefix_search(ErlNifEnv* env, int argc,
                                          const ERL_NIF_TERM argv[])
{
    art_tree* t;
    ErlNifBinary key;
    callback_data cb_data;
    
    // extract arguments atr_tree, key
    if(!enif_get_resource(env, argv[0], elibart_RESOURCE, (void**) &t))
        return enif_make_badarg(env);
    if (!enif_inspect_binary(env, argv[1], &key))
        return enif_make_badarg(env);

    cb_data.env = env;
    if(!enif_is_pid(env, argv[3]))
        return -1;

    if(!enif_get_local_pid(env, argv[3], &cb_data.pid))
        return -1;

    cb_data.msg_env = enif_alloc_env();
    if(cb_data.msg_env == NULL)
        return -1;

    cb_data.caller_ref = enif_make_copy(cb_data.msg_env, argv[2]);
    ERL_NIF_TERM res = enif_make_tuple2(cb_data.msg_env, cb_data.caller_ref, mk_atom(cb_data.msg_env, "ok"));    

    // TODO this should be a worker thread since it's a long opearation (?)
    if (art_iter_prefix(t, (char *) key.data, key.size, prefix_cb, &cb_data) || 
        !enif_send(cb_data.env, &cb_data.pid, cb_data.msg_env, res))
    {
        enif_free(cb_data.msg_env);

        return enif_make_atom(env, "error");
    }

    enif_free(cb_data.msg_env);

    return enif_make_atom(env, "ok");
}

static ERL_NIF_TERM elibart_size(ErlNifEnv* env, int argc,
                                          const ERL_NIF_TERM argv[])
{
    art_tree* t;

    // extract arguments atr_tree
    if(!enif_get_resource(env, argv[0], elibart_RESOURCE, (void**) &t))
        return enif_make_badarg(env);

    uint64_t size = art_size(t);

    return enif_make_int64(env, size);
}

static void elibart_resource_cleanup(ErlNifEnv* env, void* arg)
{
    /* Delete any dynamically allocated memory stored in elibart_handle */
    /* elibart_handle* handle = (elibart_handle*)arg; */
    art_tree* t = (art_tree*) arg;

    destroy_art_tree(t);
}

static int on_load(ErlNifEnv* env, void** priv_data, ERL_NIF_TERM load_info)
{
    ErlNifResourceFlags flags = ERL_NIF_RT_CREATE | ERL_NIF_RT_TAKEOVER;
    ErlNifResourceType* rt = enif_open_resource_type(env, NULL,
                                                     "elibart_resource",
                                                     &elibart_resource_cleanup,
                                                     flags, NULL);
    if (rt == NULL)
        return -1;

    elibart_RESOURCE = rt;

    return 0;
}

ERL_NIF_INIT(elibart, nif_funcs, &on_load, NULL, NULL, NULL);


