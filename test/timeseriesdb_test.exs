defmodule TimeSeriesDBTest do
  use ExUnit.Case
  doctest TimeSeriesDB

  alias TimeSeriesDB.State

  setup do
    File.rm_rf!("testdata/test_db")
    :ok
  end

  test "wrong continue" do
    {:ok, state} = State.init("testdata/test_db")

    _state =
      Enum.reduce(1..20_000, state, fn x, state ->
        State.append_row!(state, x, %{x: x, z: String.duplicate("hello", 100)})
      end)

    {:ok, state} = State.init("testdata/test_db")

    # should throw with an monotonic error
    {:error, _msg} = State.append_row(state, 1, %{x: 1, z: String.duplicate("hello", 100)})
  end

  test "backend test" do
    File.rm_rf!("testdata/test_1")
    {:ok, state} = TimeSeriesDB.State.init("testdata/test_1")

    make_element = fn x ->
      y = :erlang.phash2(x)
      %{x: x, y: :erlang.crc32("#{y}"), z: String.duplicate("#{y}", 100)}
    end

    state =
      if State.count(state) == 0 do
        Enum.reduce(1..100_000, state, fn x, state ->
          State.append_row!(state, x, make_element.(x))
        end)
      else
        state
      end

    State.flush(state)

    18 = length(State.count_files(state))
    1 = State.oldest(state)
    100_000 = State.newest(state)
    100_000 = State.count(state)

    [] = State.query_range(state, 0, 0)
    [{1, one}] = State.query_range(state, 0, 1)
    [{1, ^one}] = State.query_range(state, 1, 1)
    [{1, ^one}] = State.query_range(state, 0.1, 1.9)

    [^one] = State.query_multiple(state, [1])
    [{2, _two}] = State.query_range(state, 2, 2)

    range = [1, 10, 100, 1000, 10_000, 100_000]
    test = Enum.map(range, make_element)
    ^test = State.query_multiple(state, range)
    [nil, nil, nil | ^test] = State.query_multiple(state, [1.5, 10.1, 100.2 | range])

    [{100_000, one}] = State.query_range(state, 100_000, 100_000)
    [^one] = State.query_multiple(state, [100_000])

    order = :lists.seq(90_000, 95_000)
    ^order = State.query_range(state, 90_000, 95_000) |> Enum.map(fn {k, _} -> k end)
  end

  test "frontend test" do
    File.rm_rf!("testdata/test_1")
    {:ok, pid} = TimeSeriesDB.start_link("testdata/test_1")

    make_element = fn x ->
      y = :erlang.phash2(x)
      %{x: x, y: :erlang.crc32("#{y}"), z: String.duplicate("#{y}", 100)}
    end

    if TimeSeriesDB.count(pid) == 0 do
      for x <- 1..100_000 do
        TimeSeriesDB.append_row(pid, x, make_element.(x))
      end
    end

    TimeSeriesDB.flush(pid)

    18 = length(TimeSeriesDB.count_files(pid))
    1 = TimeSeriesDB.oldest(pid)
    100_000 = TimeSeriesDB.newest(pid)
    100_000 = TimeSeriesDB.count(pid)

    [] = TimeSeriesDB.query_range(pid, 0, 0)
    [{1, one}] = TimeSeriesDB.query_range(pid, 0, 1)
    [{1, ^one}] = TimeSeriesDB.query_range(pid, 1, 1)
    [{1, ^one}] = TimeSeriesDB.query_range(pid, 0.1, 1.9)

    [^one] = TimeSeriesDB.query_multiple(pid, [1])
    [{2, _two}] = TimeSeriesDB.query_range(pid, 2, 2)

    range = [1, 10, 100, 1000, 10_000, 100_000]
    test = Enum.map(range, make_element)
    ^test = TimeSeriesDB.query_multiple(pid, range)
    [nil, nil, nil | ^test] = TimeSeriesDB.query_multiple(pid, [1.5, 10.1, 100.2 | range])

    [{100_000, one}] = TimeSeriesDB.query_range(pid, 100_000, 100_000)
    [^one] = TimeSeriesDB.query_multiple(pid, [100_000])

    order = :lists.seq(90_000, 95_000)
    ^order = TimeSeriesDB.query_range(pid, 90_000, 95_000) |> Enum.map(fn {k, _} -> k end)
  end
end
