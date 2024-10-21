#!/usr/bin/env elixir

make_element = fn x ->
  y = :erlang.phash2(x)
  %{x: x, y: :erlang.crc32("#{y}"), z: String.duplicate("#{y}", 100)}
end

File.rm_rf!("testdata/bench_tdb")
File.rm_rf!("testdata/bench_dets")

{:ok, state} = TimeSeriesDB.start_link("testdata/bench_tdb")
{:ok, dets} = DetsPlus.open_file(:bench_dets, file: "testdata/bench_dets")

for x <- 1..100_000 do
  DetsPlus.insert(dets, {x, make_element.(x)})
  TimeSeriesDB.append_row(state, x, make_element.(x))
end

TimeSeriesDB.flush(state)
DetsPlus.sync(dets)

keys = [1, 10, 100, 1000, 10_000, 100_000]

dets_multiple = fn dets, range ->
  Enum.map(range, fn key ->
    with [{_, value}] <- DetsPlus.lookup(dets, key) do
      value
    end
  end)
end

ref = TimeSeriesDB.query_multiple(state, keys) |> IO.inspect()
^ref = dets_multiple.(dets, keys)

Benchee.run(%{
  "TimeSeriesDB.query_multiple" => fn -> TimeSeriesDB.query_multiple(state, keys) end,
  "DetsPlus.lookup" => fn -> dets_multiple.(dets, keys) end
})
