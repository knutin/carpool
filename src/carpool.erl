-module(carpool).
-behaviour(gen_server).
-include_lib("eunit/include/eunit.hrl").

%% API
-export([connect/1, connect/2]).
-export([disconnect/1, disconnect/2]).
-export([claim/3, workers/1]).

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
    connect(Pool, self()).

connect(Pool, Pid) ->
    gen_server:call(?MODULE, {connect, Pool, Pid}).

disconnect(Pool) ->
    disconnect(Pool, self()).

disconnect(Pool, Pid) ->
    gen_server:call(?MODULE, {disconnect, Pool, Pid}).


claim(Pool, F, Timeout) ->
    claim(Pool, F, Timeout, os:timestamp(), 0, next_slot(Pool)).

claim(Pool, F, Timeout, StartTime, Misses, Slot) ->
    case catch ets:slot(?TABLE, Slot) of
        [{worker, {Pool, Pid}, _, 0}] ->
            case ets:update_counter(?TABLE, {Pool, Pid},
                                    [{?LOCK_POS, 0},
                                     {?LOCK_POS, 1, 1, 1}]) of
                [0, 1] ->
                    ElapsedUs = timer:now_diff(os:timestamp(), StartTime),
                    try
                        {ok, F(Pid, ElapsedUs, Misses)}
                    after
                        [0] = ets:update_counter(?TABLE, {Pool, Pid},
                                                 [{?LOCK_POS, 0, 0, 0}])
                    end;

                [1, 1] ->
                    case timer:now_diff(os:timestamp(), StartTime) > Timeout of
                        true ->
                            {error, claim_timeout};
                        false ->
                            claim(Pool, F, Timeout, StartTime, Misses + 1, next_slot(Pool))
                    end
            end;

        [{worker, _, _, 1}] ->
            case timer:now_diff(os:timestamp(), StartTime) > Timeout of
                true ->
                    {error, claim_timeout};
                false ->
                    claim(Pool, F, Timeout, StartTime, Misses + 1, next_slot(Pool))
            end
    end.
%%
%% INTERNAL API
%%

next_slot(Pool) ->
    next_slot(Pool,
              erlang:system_info(scheduler_id),
              erlang:system_info(schedulers_online)).

next_slot(Pool, Partition, Partitions) ->
    Size = worker_count(Pool),
    PartitionSize = Size div Partitions,
    MaxValue = PartitionSize * Partition,
    MinValue = MaxValue - PartitionSize,

    ets:update_counter(?SLOT_TABLE, {Pool, Partition}, {2, 1, MaxValue-1, MinValue}).



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
    ets:new(?TABLE, [public,
                     ordered_set,
                     {keypos, 2},
                     {read_concurrency, true},
                     {write_concurrency, true},
                     named_table]),

    ets:new(?SLOT_TABLE, [public,
                          ordered_set,
                          {keypos, 1},
                          {read_concurrency, true},
                          {write_concurrency, true},
                          named_table]),

    ets:new(?SIZE_TABLE, [public,
                          ordered_set,
                          {keypos, 1},
                          {read_concurrency, true},
                          named_table]),

    {ok, #state{}}.

handle_call({connect, Pool, Pid}, _From, State) ->
    case ets:member(?TABLE, {Pool, Pid}) of
        false ->
            Monitor = erlang:monitor(process, Pid),
            true = ets:insert_new(?TABLE, {worker, {Pool, Pid}, Monitor, 0}),
            ets:insert(?SIZE_TABLE, {Pool, length(workers(Pool))}),

            Size = worker_count(Pool),
            PartitionSize = Size div erlang:system_info(schedulers),

            lists:foreach(fun (Slot) ->
                                  MaxValue = PartitionSize * Slot,
                                  MinValue = MaxValue - PartitionSize,
                                  ets:insert(?SLOT_TABLE, {{Pool, Slot}, MinValue})
                          end, lists:seq(1, erlang:system_info(schedulers))),
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
                    lists:foreach(fun (Slot) ->
                                          ets:delete(?SLOT_TABLE, {Pool, Slot})
                                  end, lists:seq(1, erlang:system_info(schedulers)));
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
      ?_test(test_next_slot()),
      ?_test(test_monitor()),
      ?_test(test_already_connected()),
      ?_test(test_disconnect()),
      {timeout, 60, ?_test(test_performance())}
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

    F = fun (Pid, _, _) ->
                Pid ! {self(), do_work, 100},
                receive {result, Pid} -> Pid end
        end,

    %% claim(Pool, F, Timeout, StartTime, Misses, Slot)
    ?assertEqual({ok, Worker1}, claim(Pool, F, 100, os:timestamp(), 0, 0)),
    ?assertEqual({ok, Worker2}, claim(Pool, F, 100, os:timestamp(), 0, 1)),
    ?assertEqual({ok, Worker1}, claim(Pool, F, 100, os:timestamp(), 0, 0)),

    [exit(P, kill) || P <- [Worker1, Worker2]],
    timer:sleep(10),
    ?assertEqual([], workers(Pool)).


test_timeout() ->
    Pool = test_pool,
    Worker = spawn(?MODULE, worker_loop, [Pool, self()]),
    receive connected -> ok end,
    ?assertEqual([{Worker, 0}], workers(Pool)),

    Parent = self(),
    F = fun (Pid, _, _) ->
                Pid ! {self, do_work, infinity},
                Parent ! go,
                receive M -> throw({unexpected_message, M}) end
        end,
    spawn(fun () -> claim(Pool, F, 100) end),
    receive go -> ok end,

    ?assertEqual([{Worker, 1}], workers(Pool)),

    Start = os:timestamp(),
    TimeoutUsec = 100,
    ?assertEqual({error, claim_timeout},
                 claim(Pool, fun (_) -> ok end, TimeoutUsec)),
    ?assert(timer:now_diff(os:timestamp(), Start) > TimeoutUsec),
    exit(Worker, kill),
    ok = disconnect(Pool, Worker),
    ?assertEqual([], ets:tab2list(?TABLE)).


test_next_slot() ->
    Pool = next_slot_pool,
    Workers = [begin
                   W = spawn(?MODULE, worker_loop, [Pool, self()]),
                   receive connected -> ok end,
                   W
               end || _ <- lists:seq(1, 32)],

    32 = worker_count(Pool),

    ?assertEqual([0, 1, 2, 3],
                 lists:usort(
                   lists:map(fun (_) ->
                                     next_slot(Pool, 1, 8)
                             end, lists:seq(1, 64)))),

    [disconnect(Pool, W) || W <- Workers],
    ok.

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
    timer:sleep(10),
    ?assertEqual([], workers(Pool)).


test_already_connected() ->
    [] = workers(p),
    ok = connect(p),
    [{_, 0}] = workers(p),
    1 = worker_count(p),
    {error, already_connected} = connect(p),
    [{_, 0}] = workers(p),
    1 = worker_count(p),
    ok = disconnect(p).



test_disconnect() ->
    ?assertEqual([], workers(p1)),
    ?assertEqual(ok, connect(p1)),
    ?assertEqual([{self(), 0}], workers(p1)),
    ?assertEqual(ok, disconnect(p1)),
    ?assertEqual([], workers(p1)),
    ?assertEqual({error, pool_not_found}, worker_count(p1)).


test_performance() ->
    test_performance(1, 1, 100000),
    test_performance(1, 4, 100000),
    test_performance(8, 8, 100000),
    test_performance(32, 32, 10000),
    test_performance(256, 32, 10000),
    test_performance(256, 256, 10000).

test_performance(PoolSize, CallerSize, Num) ->
    Pool = perf_pool,
    ?assertEqual([], ets:tab2list(?TABLE)),
    Workers = [begin
                   W = spawn(?MODULE, worker_loop, [Pool, self()]),
                   receive connected -> ok end,
                   W
               end || _ <- lists:seq(1, PoolSize)],

    Req = fun (_, _ElapsedUs, _Misses) ->
                  true
          end,

    Parent = self(),

    ClaimLoop = fun (_, N) when Num =:= N ->
                         ok;
                     (F, N) ->
                        case claim(Pool, Req, 100) of
                            {ok, true} ->
                                F(F, N+1);
                            {error, claim_timeout} ->
                                F(F, N)
                        end
                 end,

    CallerF = fun () ->
                      receive go -> ok end,
                      Start = os:timestamp(),
                      ok = ClaimLoop(ClaimLoop, 0),
                      Parent ! {self(), done, timer:now_diff(os:timestamp(),
                                                             Start)}
              end,

    Claimers = [spawn(CallerF) || _ <- lists:seq(1, CallerSize)],
    [Pid ! go || Pid <- Claimers],

    ElapsedUs = lists:map(fun (Pid) -> receive {Pid, done, Us} -> Us end end,
                          Claimers),

    error_logger:info_msg("pool size: ~p, callers: ~p, claims: ~p~n"
                          "avg per claim: ~.2f us, max: ~p us~n",
                          [PoolSize, CallerSize, Num,
                           lists:sum(ElapsedUs) / (Num * length(Claimers)),
                           lists:max(ElapsedUs)]),

    [disconnect(Pool, W) || W <- Workers].
