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

volume_insert_10M_test_() ->
    {timeout, 60, ?_assertEqual(?MAX + 4, begin
                                              Ref = get("art"),
                                              put("art", Ref),
                                              insert_n(Ref, 0, ?MAX),
                                              art_size(Ref)
                                          end)}.

insert_n(Ref, N, M) ->
    if
        N =:= M -> ok;
        true ->
            Random = term_to_binary(random:seed(now())),
            Prefix = list_to_binary(integer_to_list(M)),
            Key = <<Prefix/binary,Random/binary,?LONG_PREFIX/binary,Random/binary,?LONG_PREFIX/binary>>,
            {ok, _} = insert(Ref, Key, <<"">>),
            insert_n(Ref, N, M - 1)
    end.

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

multithread_search_10x1K_test_() ->
    Ref = get("art"),
    {spawn, lists:duplicate(1000, search_worker(Ref))}.

insert_worker(Ref) ->
    insert_n(Ref, 4500000, 5500000).

search_worker(Ref) ->
    L = prefix_search(Ref, <<"4000">>),
    ?assertEqual(1111, length(L)),
    ?_assertEqual(empty, search(Ref, <<"trash">>)).

-endif.
