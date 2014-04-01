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

%% This cannot be a separate function. Code must be inline to trigger
%% Erlang compiler's use of optimized selective receive.
-define(WAIT_FOR_REPLY(Ref, TOut),
        receive 
          {Ref, ok} ->
            ok;
          {Ref, {ok, _L} = R} ->
            R;
          {Ref, Bin} ->
                Bin
        after
            TOut -> {error, timeout}
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

async_prefix_search(_Ref, _Prefix, _CallerRef, _Pid) ->
  ?nif_stub.

-spec prefix_search(Ref :: term(), Prefix :: binary()) ->
  {ok, term()} |
  {error, Reason :: term()}.

prefix_search(Ref, Prefix) ->
  CallerRef = make_ref(),
  ok = async_prefix_search(Ref, Prefix, CallerRef, self()),
  Reply = ?WAIT_FOR_REPLY(CallerRef, 1000),
  case Reply of
    {ok, Res} ->
      Res;
    {error, _} = Error ->
      Error
  end.

-spec fold(Ref :: term(), Prefix :: binary(), Fun :: function(), Acc :: term()) ->
  {ok, term()} |
  {error, Reason :: term()}.

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
  Res = prefix_search(Ref, <<"api">>),
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
  L = prefix_search(Ref, <<"4000">>),
  ?assertEqual(1111, length(L)).

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
  L = prefix_search(Ref, <<"45000">>),
  ?assertEqual(111, length(L)),
  ?assertEqual(empty, search(Ref, <<"trash">>)),
  ?assertEqual({ok, Key}, search(Ref, Key)).

-endif.
