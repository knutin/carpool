-module(carpool).
-behaviour(gen_server).
-include_lib("eunit/include/eunit.hrl").

%% API
-export([connect/1, disconnect/1, claim/3, workers/1]).

-export([start_link/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

%% Exported for testing
-export([worker_loop/2]).

-record(state, {}).

-define(TABLE, pools).
-define(SIZE_TABLE, pool_sizes).
-define(SLOT_TABLE, pool_slots).
-define(LOCK_POS, 4).

%%
%% API
%%

connect(Pool) ->
    gen_server:call(?MODULE, {connect, Pool, self()}).

disconnect(Pool) ->
    gen_server:call(?MODULE, {disconnect, Pool, self()}).



claim(Pool, F, Timeout) ->
    claim(Pool, F, Timeout, os:timestamp(), next_slot(Pool)).

claim(Pool, F, Timeout, StartTime, Slot) ->
    case ets:slot(?TABLE, Slot-1) of
        [{worker, {Pool, Pid}, _, 0}] ->
            case ets:update_counter(?TABLE, {Pool, Pid},
                                    [{?LOCK_POS, 0},
                                     {?LOCK_POS, 1, 1, 1}]) of
                [0, 1] ->
                    try
                        {ok, F(Pid)}
                    after
                        [0] = ets:update_counter(?TABLE, {Pool, Pid},
                                                 [{?LOCK_POS, 0, 0, 0}])
                    end;

                [1, 1] ->
                    case timer:now_diff(os:timestamp(), StartTime) > Timeout of
                        true ->
                            {error, claim_timeout};
                        false ->
                            claim(Pool, F, Timeout, StartTime, next_slot(Pool))
                    end
            end;

        [{worker, _, _, 1}] ->
            case timer:now_diff(os:timestamp(), StartTime) > Timeout of
                true ->
                    {error, claim_timeout};
                false ->
                    claim(Pool, F, Timeout, StartTime, next_slot(Pool))
            end
    end.


%%
%% INTERNAL API
%%

next_slot(Pool) ->
    Size = worker_count(Pool),
    ets:update_counter(?SLOT_TABLE, Pool, {2, 1, Size, 1}).

worker_count(Pool) ->
    case ets:lookup(?SIZE_TABLE, Pool) of
        [{Pool, Size}] ->
            Size;
        [] ->
            {error, pool_not_found}
    end.

workers(Pool) ->
    [{Worker, State} || {worker, {P, Worker}, _, State} <- ets:tab2list(?TABLE),
                        Pool == P].



start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).


%%
%% gen server
%%

init([]) ->
    ets:new(?TABLE, [public, ordered_set, {keypos, 2},
                     {read_concurrency, true}, named_table]),

    ets:new(?SLOT_TABLE, [public, ordered_set, {keypos, 1},
                          {read_concurrency, true}, named_table]),

    ets:new(?SIZE_TABLE, [public, ordered_set, {keypos, 1},
                          {read_concurrency, true}, named_table]),

    {ok, #state{}}.

handle_call({connect, Pool, Pid}, _From, State) ->
    case ets:member(?TABLE, {Pool, Pid}) of
        false ->
            Monitor = erlang:monitor(process, Pid),
            true = ets:insert_new(?TABLE, {worker, {Pool, Pid}, Monitor, 0}),

            case ets:member(?SLOT_TABLE, Pool) of
                false ->
                    true = ets:insert_new(?SLOT_TABLE, {Pool, 0});
                true ->
                    true
            end,
            ets:insert(?SIZE_TABLE, {Pool, length(workers(Pool))}),
            {reply, ok, State};
        true ->
            {reply, {error, already_connected}, State}
    end;

handle_call({disconnect, Pool, Pid}, _From, State) ->
    case ets:member(?TABLE, {Pool, Pid}) of
        true ->
            ets:delete(?TABLE, {Pool, Pid}),
            case length(workers(Pool)) of
                0 ->
                    ets:delete(?SIZE_TABLE, Pool),
                    ets:delete(?SLOT_TABLE, Pool);
                N ->
                    ets:insert(?SIZE_TABLE, {Pool, N})
            end,
            {reply, ok, State};
        false ->
            {reply, {error, not_connected}, State}
    end.


handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({'DOWN', Monitor, process, Pid, _Reason}, State) ->
    Match = {worker, {'_', Pid}, Monitor, '_'},
    Guard = [],
    Return = ['$_'],
    case ets:select(?TABLE, [{Match, Guard, Return}]) of
        [{worker, {Pool, Pid}, Monitor, _}] ->
            ets:delete(?TABLE, {Pool, Pid}),
            ets:insert(?SIZE_TABLE, {Pool, length(workers(Pool))}),
            {noreply, State};
        [] ->
            {noreply, State}
    end.


terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%%
%% TESTS
%%


integration_test_() ->
    {setup, fun setup/0, fun teardown/1,
     [
      ?_test(test_multiple_workers()),
      ?_test(test_timeout()),
      ?_test(test_monitor()),
      ?_test(test_already_connected()),
      ?_test(test_disconnect())
     ]}.


setup() ->
    application:start(carpool).


teardown(_) ->
    application:stop(carpool).


worker_loop(Pool, Parent) ->
    ok = connect(Pool),
    Parent ! connected,
    do_worker_loop().

do_worker_loop() ->
    receive {From, do_work, Sleep} ->
            timer:sleep(Sleep),
            From ! {result, self()},
            do_worker_loop()
    end.


test_multiple_workers() ->
    Pool = test_pool,

    [Worker1, Worker2] = [spawn(?MODULE, worker_loop, [Pool, self()]),
                          spawn(?MODULE, worker_loop, [Pool, self()])],
    receive connected -> ok end,
    receive connected -> ok end,
    ?assertEqual([{Worker1, 0}, {Worker2, 0}], workers(Pool)),

    F = fun (Pid) ->
                Pid ! {self(), do_work, 100},
                receive {result, Pid} -> Pid end
        end,

    ?assertEqual({ok, Worker1}, claim(Pool, F, 100)),
    ?assertEqual({ok, Worker2}, claim(Pool, F, 100)),
    ?assertEqual({ok, Worker1}, claim(Pool, F, 100)),

    [exit(P, kill) || P <- [Worker1, Worker2]],
    timer:sleep(10),
    ?assertEqual([], workers(Pool)).


test_timeout() ->
    Pool = test_pool,
    Worker = spawn(?MODULE, worker_loop, [Pool, self()]),
    receive connected -> ok end,
    ?assertEqual([{Worker, 0}], workers(Pool)),

    F = fun (Pid) ->
                Pid ! {self, do_work, infinity},
                receive M -> throw({unexpected_message, M}) end
        end,
    Parent = self(),
    spawn(fun () -> Parent ! go, claim(Pool, F, 100) end),
    receive go -> ok end,

    ?assertEqual([{Worker, 1}], workers(Pool)),

    Start = os:timestamp(),
    TimeoutUsec = 100,
    ?assertEqual({error, claim_timeout},
                 claim(Pool, fun (_) -> ok end, TimeoutUsec)),
    ?assert(timer:now_diff(os:timestamp(), Start) > TimeoutUsec).


test_monitor() ->
    Pool = monitor,
    Parent = self(),
    Worker = spawn(fun () ->
                           ok = connect(Pool),
                           Parent ! connected,
                           timer:sleep(infinity)
                   end),
    Monitor = erlang:monitor(process, Worker),
    receive connected -> ok end,
    ?assertEqual([{Worker, 0}], workers(Pool)),

    exit(Worker, kill),

    receive M1 ->
            ?assertEqual({'DOWN', Monitor, process, Worker, killed}, M1)
    end,
    ?assertEqual([], workers(Pool)).


test_already_connected() ->
    ?assertEqual([], workers(p)),
    ?assertEqual(ok, connect(p)),
    ?assertEqual([{self(), 0}], workers(p)),
    ?assertEqual(1, worker_count(p)),
    ?assertEqual({error, already_connected}, connect(p)),
    ?assertEqual([{self(), 0}], workers(p)),
    ?assertEqual(1, worker_count(p)).


test_disconnect() ->
    ?assertEqual([], workers(p1)),
    ?assertEqual(ok, connect(p1)),
    ?assertEqual([{self(), 0}], workers(p1)),
    ?assertEqual(ok, disconnect(p1)),
    ?assertEqual([], workers(p1)),
    ?assertEqual({error, pool_not_found}, worker_count(p1)).
