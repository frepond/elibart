-module(elibart).

-export([new/0,
         insert/3,
         search/2,
         art_size/1,
         prefix_search/3]).

-on_load(init/0).

-define(nif_stub, nif_stub_error(?LINE)).
nif_stub_error(Line) ->
    erlang:nif_error({nif_not_loaded,module,?MODULE,line,Line}).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% This cannot be a separate function. Code must be inline to trigger
%% Erlang compiler's use of optimized selective receive.
-define(WAIT_FOR_REPLY(Ref),
        receive {Ref, Reply} ->
                Reply;
                ok -> ok
        after
            100 -> error %% TODO, this is a rather clumsy method to detect no more results
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


%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

basic_test() ->
  {ok, Ref} = new(),
  ?assertEqual({ok, empty}, insert(Ref, <<"test:element">>, <<"some_value">>)),
  ?assertEqual({ok, <<"some_value">>}, insert(Ref, <<"test:element">>, <<"some_new_value">>)),
  ?assertEqual({ok, <<"some_new_value">>}, insert(Ref, <<"test:element">>, <<"012345678901234567">>)),
  ?assertEqual(empty, search(Ref, <<"trash">>)),
  insert(Ref, <<"empty">>,<<>>),
  ?assertEqual({ok, <<"012345678901234567">>}, search(Ref, <<"test:element">>)).

volume_test() ->
  {ok, Ref} = new(),
  Max = 5000000,
  insert_n(Ref, Max),
  ?assertEqual(Max, art_size(Ref)),
  Key = list_to_binary(integer_to_list(1)),
  ?assertEqual({ok, Key}, search(Ref, Key)),
  Key1 = list_to_binary(integer_to_list(Max)),
  ?assertEqual({ok, Key1}, search(Ref, Key1)),
  Key2 = list_to_binary(integer_to_list(Max div 2)),
  ?assertEqual({ok, Key2}, search(Ref, Key2)).

insert_n(Ref, N) ->
  if 
    N =:= 0 -> ok;
    true -> 
      Key = list_to_binary(integer_to_list(N)),
      {ok, empty} = insert(Ref, Key, Key),
      insert_n(Ref, N - 1)
  end.

prefix_test() ->
  {ok, Ref} = new(),
  insert(Ref, <<"api.leapsight.test">>, <<"api.leapsight.test">>),
  insert(Ref, <<"api.leapsight">>, <<"api.leapsight">>),
  insert(Ref, <<"api">>, <<"api">>),
  insert(Ref, <<"apa.leapsight.test">>, <<"apa.leapsight.test">>),
  prefix_search(Ref, <<"api">>, fun prefix_fun/2).

prefix_fun(Key, Value) ->
  List = [{<<"api">>, <<"api">>}, {<<"api.leapsight">>, <<"api.leapsight">>}, 
            {<<"api.leapsight.test">>, <<"api.leapsight.test">>}],
  ?assertEqual(Key, Value),
  ?assert(lists:keymember(Key, 1, List)).

-endif.
