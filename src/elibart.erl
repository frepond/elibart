-module(elibart).

-export([new/0,
         destroy/1,
         insert/3,
         search/2,
         art_size/1,
         prefix_search/3,
         fold/4]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-on_load(init/0).

-define(nif_stub, nif_stub_error(?LINE)).
nif_stub_error(Line) ->
    erlang:nif_error({nif_not_loaded,module,?MODULE,line,Line}).

%% This cannot be a separate function. Code must be inline to trigger
%% Erlang compiler's use of optimized selective receive.
-define(WAIT_FOR_REPLY(Ref),
        receive {Ref, Reply} ->
                Reply
        after
            100 -> error
        end).

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

async_prefix_search(_Ref, _Key, _CallerRef, _Pid) ->
  ?nif_stub.

prefix_search(Ref, Key, Fun) ->
  CallerRef = make_ref(),
  ok = async_prefix_search(Ref, Key, CallerRef, self()),
  process_results(CallerRef, Fun).

process_results(CallerRef, Fun) ->
  Res = ?WAIT_FOR_REPLY(CallerRef),
  case Res of
    {Key, Value} -> 
      Fun(Key, Value),
      process_results(CallerRef, Fun);
    ok -> ok
  end.

fold(Ref, Prefix, Fun, Acc) ->
  CallerRef = make_ref(),
  ok = async_prefix_search(Ref, Prefix, CallerRef, self()),
  do_fold(Fun, Acc, CallerRef).

do_fold(Fun, Acc, CallerRef) ->
  Res = ?WAIT_FOR_REPLY(CallerRef),
  case Res of
    {Key, Value} -> 
      do_fold(Fun, Fun({Key, Value}, Acc), CallerRef);
    ok -> 
      Acc
  end.

collect(Ref, Prefix) ->
  fold(Ref, Prefix, fun(KV, Acc) -> [KV | Acc] end, []).


%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

-define(MAX, 5000000).

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
  Res = collect(Ref, <<"api">>),
  ?assert(lists:all(fun({K, _V}) -> lists:keymember(K, 1, List) end, Res)),
  ?assertEqual(3, length(Res)).

volume_insert_5M_test() ->
  Ref = get("art"),
  put("art", Ref),
  insert_n(Ref, 0, ?MAX),
  ?assertEqual(?MAX + 4, art_size(Ref)).

insert_n(Ref, N, M) ->
  if 
    N =:= M -> ok;
    true -> 
      Key = list_to_binary(integer_to_list(M)),
      {ok, _} = insert(Ref, Key, Key),
      insert_n(Ref, N, M - 1)
  end.

volume_search_5M_test() ->
  Ref = get("art"),
  Key = list_to_binary(integer_to_list(1)),
  ?assertEqual({ok, Key}, search(Ref, Key)),
  Key1 = list_to_binary(integer_to_list(?MAX)),
  ?assertEqual({ok, Key1}, search(Ref, Key1)),
  Key2 = list_to_binary(integer_to_list(?MAX div 2)),
  ?assertEqual({ok, Key2}, search(Ref, Key2)).

volume_prefix_1K_test() ->
  Ref = get("art"),
  put("res", []),
  prefix_search(Ref, <<"4000">>, fun volume_prefix_fun/2),
  L = get("res"),
  ?assertEqual(1111, length(L)),
  erase("res").

volume_prefix_fun(Key, _Value) ->
  L = get("res"),
  put("res", [Key | L]).

multithread_search_test() ->
  Ref = get("art"),
  {spawn, [ insert_worker(Ref),
            search_worker(Ref),
            search_worker(Ref),
            search_worker(Ref),
            search_worker(Ref),
            search_worker(Ref),
            search_worker(Ref),
            search_worker(Ref),
            search_worker(Ref),
            search_worker(Ref),
            search_worker(Ref)]}.

insert_worker(Ref) ->
  insert_n(Ref, 4500000, 5500000).

search_worker(Ref) ->
  Key = <<"100">>,
  L = fold(Ref, <<"45000">>, fun (KV, Acc) -> [KV | Acc] end, []),
  ?assertEqual(111, length(L)),
  ?assertEqual(empty, search(Ref, <<"trash">>)),
  ?assertEqual({ok, Key}, search(Ref, Key)).

-endif.
