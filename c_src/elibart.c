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

#define BUFF_SIZE 262144

static ErlNifResourceType* elibart_RESOURCE = NULL;

typedef struct
{
    size_t size;
    unsigned char *data;
} art_elem_struct;

typedef struct
{
    ErlNifEnv *env;
    ERL_NIF_TERM t;
    ERL_NIF_TERM key;
    ERL_NIF_TERM result_acc;
} callback_data;



// Prototypes
static ERL_NIF_TERM elibart_new(ErlNifEnv* env, int argc,
                                   const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM elibart_destroy(ErlNifEnv* env, int argc,
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
    fprintf(stderr, "%lu|", bin->size);
    fflush(stderr);
    fwrite(bin->data, 1, bin->size, stderr);
    fputs("|\n", stderr);
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
    {"destroy", 1, elibart_destroy, ERL_NIF_DIRTY_JOB_CPU_BOUND},
    {"insert", 3, elibart_insert},
    {"search", 2, elibart_search},
    {"async_prefix_search", 2, elibart_prefix_search, ERL_NIF_DIRTY_JOB_CPU_BOUND},
    {"art_size", 1, elibart_size}
};

static ERL_NIF_TERM elibart_new(ErlNifEnv* env, int argc,
                                   const ERL_NIF_TERM argv[])
{
    art_tree* t = enif_alloc_resource(elibart_RESOURCE,
                                                    sizeof(art_tree));

    if (init_art_tree(t) != 0)
        return mk_error(env, "init_art_tree");
    else
    {
        ERL_NIF_TERM res = enif_make_resource(env, t);
        enif_release_resource(t);

        return enif_make_tuple2(env, mk_atom(env, "ok"), res);
    }
}

static int delete_cb(void *data, const unsigned char *k, uint32_t k_len, void *val)
{
    art_elem_struct *elem = val;

    free(elem->data);
    free(elem);

    return 0;
}

static ERL_NIF_TERM elibart_destroy(ErlNifEnv* env, int argc,
                                   const ERL_NIF_TERM argv[])
{
    art_tree *t;

    if (argc != 1)
        return enif_make_badarg(env);
    if(!enif_get_resource(env, argv[0], elibart_RESOURCE, (void**) &t))
        return enif_make_badarg(env);

    art_iter(t, delete_cb, NULL);

    if (destroy_art_tree((art_tree*) argv[0]) != 0)
        return mk_error(env, "destroy_art_tree");

    return mk_atom(env, "ok");
}

static ERL_NIF_TERM elibart_insert(ErlNifEnv* env, int argc,
                                          const ERL_NIF_TERM argv[])
{
    art_tree* t;
    ErlNifBinary key, value;
    art_elem_struct *elem;

    // extract arguments atr_tree, key, value
    if (argc != 3)
        return enif_make_badarg(env);
    if(!enif_get_resource(env, argv[0], elibart_RESOURCE, (void**) &t))
        return enif_make_badarg(env);
    if (!enif_inspect_binary(env, argv[1], &key))
        return enif_make_badarg(env);
    if (!enif_inspect_binary(env, argv[2], &value))
        return enif_make_badarg(env);

    //create art element
    elem = malloc(sizeof(art_elem_struct));

    if (elem == NULL)
        return mk_error(env, "malloc_no_mem");

    elem->data = malloc(value.size);

    if (elem->data == NULL)
        return mk_error(env, "malloc_no_mem");

    elem->size = value.size;
    memcpy(elem->data, value.data, value.size);

    // insert the element in the art_tree
    art_elem_struct *old_elem = art_insert(t, key.data, key.size, elem);

    // the inserted key is new
    if (!old_elem)
        return enif_make_tuple2(env, mk_atom(env, "ok"), mk_atom(env, "empty"));

    // the inserted key already existed, return previous value
    ErlNifBinary res;
    enif_alloc_binary(old_elem->size, &res);
    memcpy(res.data, old_elem->data, old_elem->size);

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
    if(argc != 2)
      return enif_make_badarg(env);
    if(!enif_get_resource(env, argv[0], elibart_RESOURCE, (void**) &t))
        return enif_make_badarg(env);
    if (!enif_inspect_binary(env, argv[1], &key))
        return enif_make_badarg(env);

    // search the art_tree for the given key
    art_elem_struct *value = art_search(t, key.data, key.size);

    // key does not exist in the art_tree
    if (!value) {
        return mk_atom(env, "empty");
    }

    // key exixts, return the associated value
    ErlNifBinary res;
    enif_alloc_binary(value->size, &res);
    memcpy(res.data, value->data, value->size);

    return enif_make_tuple2(env, mk_atom(env, "ok"), enif_make_binary(env, &res));
}

static int prefix_cb(void *data, const unsigned char *k, uint32_t k_len, void *val)
{
    callback_data *cb_data = data;
    art_elem_struct *elem = val;
    ErlNifBinary key, value;

    enif_alloc_binary(k_len, &key);
    memcpy(key.data, k, k_len);

    enif_alloc_binary(elem->size, &value);
    memcpy(value.data, elem->data, elem->size);

    ERL_NIF_TERM res = enif_make_tuple2(cb_data->env,
            enif_make_binary(cb_data->env, &key), enif_make_binary(cb_data->env, &value));
    cb_data->result_acc = enif_make_list_cell(cb_data->env, res, cb_data->result_acc);

    return 0;
}

static void* async_prefix_search(void* data)
{
    art_tree* t;
    ErlNifBinary key;
    callback_data *cb_data = (callback_data*) data;

    // TODO move to elibart_prefix_search
    // extract arguments atr_tree, key
    if(!enif_get_resource(cb_data->env, cb_data->t, elibart_RESOURCE, (void**) &t))
        return NULL; // TODO return error
    if (!enif_inspect_binary(cb_data->env, cb_data->key, &key))
        return NULL; // TODO return error

    if (art_iter_prefix(t, key.data, key.size, prefix_cb, cb_data))
        return NULL; // TODO return error

    return NULL;
}


static ERL_NIF_TERM elibart_prefix_search(ErlNifEnv* env, int argc,
                                          const ERL_NIF_TERM argv[])
{
    if (argc != 2)
        return enif_make_badarg(env);

    callback_data *cb_data = malloc(sizeof(callback_data));
    cb_data->env = env;

    cb_data->t = enif_make_copy(cb_data->env, argv[0]);
    cb_data->key = enif_make_copy(cb_data->env, argv[1]);

    ErlNifTid tid;
    ErlNifThreadOpts *opts;

    opts = enif_thread_opts_create("elibart_prefix_search");

    // We initialise the results accumulator list
    cb_data->result_acc = enif_make_list(cb_data->env, 0),

    // TODO it's not clear if this is needed it could hurt performance if many too threads are spawned.
    enif_thread_create("elibart_prefix_search", &tid, &async_prefix_search, cb_data, opts);
    enif_thread_join(tid, NULL);
    // async_prefix_search(cb_data);

    return enif_make_tuple2(cb_data->env, mk_atom(cb_data->env, "ok"), cb_data->result_acc);
}

static ERL_NIF_TERM elibart_size(ErlNifEnv* env, int argc,
                                          const ERL_NIF_TERM argv[])
{
    art_tree* t;

    // extract arguments atr_tree
    if (argc != 1)
        return enif_make_badarg(env);
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


