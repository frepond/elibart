-module(elibart).

-export([new/0,
         destroy/1,
         insert/3,
         search/2,
         art_size/1,
         prefix_search/2,
         fold/4]).

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
