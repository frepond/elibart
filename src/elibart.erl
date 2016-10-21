-module(elibart).

-export([new/0,
         destroy/1,
         insert/3,
         search/2,
         art_size/1,
         prefix_search/2,
         fold/4]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-on_load(init/0).

-define(nif_stub, nif_stub_error(?LINE)).
nif_stub_error(Line) ->
    erlang:nif_error({nif_not_loaded,module,?MODULE,line,Line}).

init() ->
    PrivDir = case code:priv_dir(?MODULE) of
                  {error, bad_name} ->
                      EbinDir = filename:dirname(code:which(?MODULE)),
                      AppPath = filename:dirname(EbinDir),
                      filename:join(AppPath, "priv");
                  Path ->
                      Path
              end,
    erlang:load_nif(filename:join(PrivDir, ?MODULE), 0).

new() ->
    ?nif_stub.

destroy(_Ref) ->
    ?nif_stub.

insert(_Ref, _Key, _Value) ->
    ?nif_stub.

search(_Ref, _Key) ->
    ?nif_stub.

art_size(_Ref) ->
    ?nif_stub.

async_prefix_search(_Ref, _Prefix) ->
    ?nif_stub.

prefix_search(Ref, Prefix) ->
    Result = async_prefix_search(Ref, Prefix),
    case Result of
        {ok, Res} ->
            Res;
        {error, _} = Error ->
            Error
    end.

fold(Ref, Prefix, Fun, Acc) ->
    case prefix_search(Ref, Prefix) of
        {error, _} = Error ->
            Error;
        Res ->
            lists:foldl(Fun, Acc, Res)
    end.

%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

-define(MAX, 10000000).
-define(LONG_PREFIX, <<"qwertyuiopasdfghjklzxcvbnmqwertyuiopasdfghjklzxcvbnmqwertyui",
                       "qwertyuiopasdfghjklzxcvbnmqwertyuiopasdfghjklzxcvbnmqwertyui",
                       "qwertyuiopasdfghjklzxcvbnmqwertyuiopasdfghjklzxcvbnmqwertyui">>).

-define(STMT_KEY, <<103,115,112,111,105,0,25,0,108,101,97,112,115,105,103,104,116,0,25,0,131,104,
                    2,100,0,7,108,115,100,95,117,114,105,109,0,0,0,22,108,115,100,58,100,101,109,
                    111,58,115,104,111,112,58,112,114,111,100,117,99,116,115>>).

basic_test() ->
    {ok, Ref} = new(),
    ?assertEqual({ok, empty}, insert(Ref, <<"test:element">>, <<"some_value">>)),
    ?assertEqual({ok, <<"some_value">>}, insert(Ref, <<"test:element">>, <<"some_new_value">>)),
    ?assertEqual({ok, <<"some_new_value">>}, insert(Ref, <<"test:element">>, <<"012345678901234567">>)),
    ?assertEqual(empty, search(Ref, <<"trash">>)),
    insert(Ref, <<"new">>,<<>>),
    ?assertEqual({ok, <<"012345678901234567">>}, search(Ref, <<"test:element">>)),
    destroy(Ref).

prefix_test() ->
    {ok, Ref} = new(),
    put("art", Ref),
    List = [{<<"api">>, <<"api">>}, {<<"api.leapsight">>, <<"api.leapsight">>},
            {<<"api.leapsight.test">>, <<"api.leapsight.test">>}, {<<"apb">>, <<>>}],
    lists:foreach(fun({K, V}) -> insert(Ref, K, V) end, List),
    Res = prefix_search(Ref, <<"api">>),
    ?assert(lists:all(fun({K, _V}) -> lists:keymember(K, 1, List) end, Res)),
    ?assertEqual(3, length(Res)).

statement_insert_test() ->
    {ok, Ref} = new(),
    put("art_stmt", Ref),
    test_support:insert_stmts(Ref).

statement_search_test() ->
    Ref = get("art_stmt"),
    Res = prefix_search(Ref, ?STMT_KEY),
    ?assertEqual(567, length(Res)).

multithread_statement_test_() ->
    Ref = get("art_stmt"),
    {spawn, lists:duplicate(1000, statement_search_worker(Ref))}.

statement_search_worker(Ref) ->
    Res = prefix_search(Ref, ?STMT_KEY),
    ?_assertEqual(567, length(Res)).

statemen_destroy_test() ->
    Ref = get("art_stmt"),
    destroy(Ref).

volume_insert_10M_test_() ->
    {timeout, 60, ?_assertEqual(?MAX + 4,
                                begin
                                    Ref = get("art"),
                                    insert_n(Ref, 0, ?MAX),
                                    art_size(Ref)
                                end)}.

insert_n(Ref, N, M) ->
    if
        N =:= M -> ok;
        true ->
            Random = term_to_binary(random:seed(now())),
            Prefix = list_to_binary(integer_to_list(M)),
            Key = <<Prefix/binary, ?LONG_PREFIX/binary, Random/binary, ?LONG_PREFIX/binary, Random/binary, ?LONG_PREFIX/binary>>,
            {ok, _} = insert(Ref, Key, <<"">>),
            insert_n(Ref, N, M - 1)
    end.


volume_prefix_test() ->
    Ref = get("art"),
    L = prefix_search(Ref, <<"4000", ?LONG_PREFIX/binary>>),
    ?assertEqual(1, length(L)).


volume_prefix_1K_test() ->
    Ref = get("art"),
    L = prefix_search(Ref, <<"4000">>),
    ?assertEqual(1111, length(L)).

volume_prefix_11K_test() ->
    Ref = get("art"),
    %% L = fold(Ref, <<"40">>, fun({K, _V}, Acc) -> [binary_to_list(K) | Acc] end, []),
    L = prefix_search(Ref, <<"400">>),
    ?assertEqual(11111, length(L)).

volume_prefix_111K_test() ->
    Ref = get("art"),
    L = prefix_search(Ref, <<"40">>),
    ?assertEqual(111111, length(L)).

%%multithread_search_10x1K_test_() ->
%%    Ref = get("art"),
%%    {spawn, lists:duplicate(1000, search_worker(Ref))}.

volume_destroy_test_() ->
    {timeout, 60, ?_assertEqual(ok,
                                begin
                                    Ref = get("art"),
                                    destroy(Ref)
                                end)}.

insert_worker(Ref) ->
    insert_n(Ref, 4500000, 5500000).

search_worker(Ref) ->
    L = prefix_search(Ref, <<"4000">>),
    ?assertEqual(1111, length(L)),
    ?_assertEqual(empty, search(Ref, <<"trash">>)).

-endif.
